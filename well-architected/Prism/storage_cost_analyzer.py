import boto3
import requests
import json
from datetime import datetime, timedelta

def get_docdb_pricing():
    """Get DocumentDB pricing from AWS pricing API"""
    try:
        pricing_url = 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonDocDB/current/index.json'
        response = requests.get(pricing_url, timeout=30)
        pricing_dict = json.loads(response.text)
        
        # Get the terms
        terms = {}
        for term_key in pricing_dict['terms']['OnDemand']:
            for term in pricing_dict['terms']['OnDemand'][term_key].values():
                sku = term['sku']
                price = list(term['priceDimensions'].values())[0]['pricePerUnit']['USD']
                terms[sku] = price
        
        # Parse pricing data
        pd = {}
        for product_key in pricing_dict['products']:
            product = pricing_dict['products'][product_key]
            
            if product.get('productFamily') == 'System Operation':
                # I/O cost
                sku = product['sku']
                region = product['attributes']['regionCode']
                price = float(terms[sku])
                pd[f'io|{region}|standard'] = {'price': price}
                pd[f'io|{region}|iopt1'] = {'price': 0.0}  # No I/O charges for optimized
                
            elif product.get('productFamily') == 'Database Storage':
                # Storage cost
                storage_usage = product['attributes'].get('usagetype', '')
                if 'StorageUsage' in storage_usage and 'Elastic' not in storage_usage:
                    sku = product['sku']
                    region = product['attributes']['regionCode']
                    volume_type = product['attributes']['volumeType']
                    price = float(terms[sku])
                    
                    if volume_type in ['IO-Optimized-DocDB', 'NVMe SSD IO-Optimized']:
                        pd[f'storage|{region}|iopt1'] = {'price': price}
                    elif volume_type in ['General Purpose', 'NVMe SSD']:
                        pd[f'storage|{region}|standard'] = {'price': price}
                        
            elif product.get('productFamily') == 'Database Instance':
                # Instance/Compute cost
                sku = product['sku']
                region = product['attributes']['regionCode']
                instance_type = product['attributes']['instanceType']
                price = float(terms[sku])
                volume_type = product['attributes']['volumeType']
                
                if volume_type in ['IO-Optimized-DocDB', 'NVMe SSD IO-Optimized']:
                    pd[f'compute|{region}|{instance_type}|iopt1'] = {'price': price}
                elif volume_type in ['General Purpose', 'NVMe SSD']:
                    pd[f'compute|{region}|{instance_type}|standard'] = {'price': price}
                    
            elif product.get('productFamily') == 'Storage Snapshot':
                # Backup storage cost
                sku = product['sku']
                region = product['attributes']['regionCode']
                price = float(terms[sku])
                pd[f'backup|{region}'] = {'price': price}
                
        return pd
        
    except Exception as e:
        # Fallback pricing (US East 1) - should match AWS Pricing API
        return {
            'io|us-east-1|standard': {'price': 0.20 / 1000000},  # $0.20 per million IOPS
            'io|us-east-1|iopt1': {'price': 0.0},  # No I/O charges for optimized
            'storage|us-east-1|standard': {'price': 0.10},  # $0.10 per GB
            'storage|us-east-1|iopt1': {'price': 0.30},  # $0.30 per GB for I/O optimized
            'backup|us-east-1': {'price': 0.095},
            # Instance pricing examples (10% premium for I/O optimized)
            'compute|us-east-1|db.r6g.xlarge|standard': {'price': 0.435},
            'compute|us-east-1|db.r6g.xlarge|iopt1': {'price': 0.4785},  # 10% premium
            'compute|us-east-1|db.r6g.large|standard': {'price': 0.218},
            'compute|us-east-1|db.r6g.large|iopt1': {'price': 0.2398}  # 10% premium
        }

def get_cluster_metrics(cluster_id, region):
    """Get CloudWatch metrics for storage cost analysis"""
    try:
        cw_client = boto3.client('cloudwatch', region_name=region)
        
        end_time = datetime.now()
        start_time = end_time - timedelta(days=30)
        
        # Get storage usage
        storage_response = cw_client.get_metric_statistics(
            Namespace='AWS/DocDB',
            MetricName='VolumeBytesUsed',
            Dimensions=[{'Name': 'DBClusterIdentifier', 'Value': cluster_id}],
            StartTime=start_time,
            EndTime=end_time,
            Period=86400,  # Daily
            Statistics=['Maximum']
        )
        
        # Get I/O operations
        read_iops_response = cw_client.get_metric_statistics(
            Namespace='AWS/DocDB',
            MetricName='VolumeReadIOPs',
            Dimensions=[{'Name': 'DBClusterIdentifier', 'Value': cluster_id}],
            StartTime=start_time,
            EndTime=end_time,
            Period=86400,
            Statistics=['Sum']
        )
        
        write_iops_response = cw_client.get_metric_statistics(
            Namespace='AWS/DocDB',
            MetricName='VolumeWriteIOPs',
            Dimensions=[{'Name': 'DBClusterIdentifier', 'Value': cluster_id}],
            StartTime=start_time,
            EndTime=end_time,
            Period=86400,
            Statistics=['Sum']
        )
        
        # Get backup storage
        backup_response = cw_client.get_metric_statistics(
            Namespace='AWS/DocDB',
            MetricName='TotalBackupStorageBilled',
            Dimensions=[{'Name': 'DBClusterIdentifier', 'Value': cluster_id}],
            StartTime=start_time,
            EndTime=end_time,
            Period=86400,
            Statistics=['Maximum']
        )
        
        # Calculate averages based on actual data available
        storage_bytes = 0
        if storage_response['Datapoints']:
            storage_bytes = max([dp['Maximum'] for dp in storage_response['Datapoints']])
            
        read_iops = 0
        read_days = 0
        if read_iops_response['Datapoints']:
            read_iops = sum([dp['Sum'] for dp in read_iops_response['Datapoints']]) / len(read_iops_response['Datapoints'])
            read_days = len(read_iops_response['Datapoints'])
            
        write_iops = 0
        write_days = 0
        if write_iops_response['Datapoints']:
            write_iops = sum([dp['Sum'] for dp in write_iops_response['Datapoints']]) / len(write_iops_response['Datapoints'])
            write_days = len(write_iops_response['Datapoints'])
            
        backup_bytes = 0
        if backup_response['Datapoints']:
            backup_bytes = max([dp['Maximum'] for dp in backup_response['Datapoints']])
        
        # Use actual days available, don't extrapolate to 30 days
        actual_days = max(read_days, write_days, 1)  # At least 1 day
        total_iops_for_period = (read_iops + write_iops) * actual_days
        
        return {
            'storage_bytes': storage_bytes,
            'read_iops_daily': read_iops,
            'write_iops_daily': write_iops,
            'backup_bytes': backup_bytes,
            'total_iops_for_period': total_iops_for_period,
            'actual_days': actual_days
        }
        
    except Exception as e:
        return None

def get_cluster_instances(cluster_id, region):
    """Get cluster instance information"""
    try:
        docdb = boto3.client('docdb', region_name=region)
        
        # Get cluster info
        cluster_response = docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)
        cluster = cluster_response['DBClusters'][0]
        instance_ids = [member['DBInstanceIdentifier'] for member in cluster['DBClusterMembers']]
        
        # Get instance details
        instances_response = docdb.describe_db_instances()
        instances = []
        
        for instance in instances_response['DBInstances']:
            if instance['DBInstanceIdentifier'] in instance_ids:
                instances.append({
                    'instance_id': instance['DBInstanceIdentifier'],
                    'instance_class': instance['DBInstanceClass']
                })
        
        return instances
        
    except Exception as e:
        return []

def analyze_storage_costs(cluster_id, region, current_storage_type='standard'):
    """Analyze storage costs and provide recommendations"""
    
    # Auto-detect storage type if caller didn't provide it
    if current_storage_type == 'standard':
        try:
            docdb = boto3.client('docdb', region_name=region)
            cl = docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)['DBClusters'][0]
            if cl.get('StorageType', '') == 'iopt1':
                current_storage_type = 'iopt1'
        except Exception:
            pass  # Fall back to 'standard' assumption

    # Get pricing, metrics, and instance info
    pricing = get_docdb_pricing()
    metrics = get_cluster_metrics(cluster_id, region)
    instances = get_cluster_instances(cluster_id, region)
    
    if not metrics:
        return None
    
    gb_bytes = 1000 * 1000 * 1000
    
    # Calculate costs based on actual data period
    storage_gb = metrics['storage_bytes'] / gb_bytes
    backup_gb = metrics['backup_bytes'] / gb_bytes
    period_iops = metrics['total_iops_for_period']
    actual_days = metrics['actual_days']
    
    # Calculate monthly equivalent (scale to 30 days for comparison)
    monthly_iops = period_iops * (30 / actual_days) if actual_days > 0 else 0
    
    # Calculate compute costs for all instances
    standard_compute_cost = 0
    iopt_compute_cost = 0
    
    for instance in instances:
        instance_class = instance['instance_class']
        # Monthly hours = 24 * 30 = 720
        standard_hourly = pricing.get(f'compute|{region}|{instance_class}|standard', {'price': 0.435})['price']
        iopt_hourly = pricing.get(f'compute|{region}|{instance_class}|iopt1', {'price': float(standard_hourly) * 1.1})['price']  # 10% premium if not found
        
        standard_compute_cost += float(standard_hourly) * 720
        iopt_compute_cost += float(iopt_hourly) * 720
    
    # Standard storage costs
    standard_io_cost = monthly_iops * pricing.get(f'io|{region}|standard', {'price': 0.20 / 1000000})['price']
    standard_storage_cost = storage_gb * pricing.get(f'storage|{region}|standard', {'price': 0.10})['price']
    backup_cost = backup_gb * pricing.get(f'backup|{region}', {'price': 0.095})['price']
    standard_total = standard_compute_cost + standard_io_cost + standard_storage_cost + backup_cost
    
    # I/O Optimized storage costs
    iopt_io_cost = monthly_iops * pricing.get(f'io|{region}|iopt1', {'price': 0.0})['price']
    iopt_storage_cost = storage_gb * pricing.get(f'storage|{region}|iopt1', {'price': 0.20})['price']
    iopt_total = iopt_compute_cost + iopt_io_cost + iopt_storage_cost + backup_cost
    
    # Generate recommendation
    recommendation = ""
    potential_savings = 0
    
    if current_storage_type == 'standard' and iopt_total < standard_total:
        potential_savings = standard_total - iopt_total
        recommendation = f"Switch to I/O Optimized storage to save ${potential_savings:.2f}/month"
    elif current_storage_type != 'standard' and standard_total < iopt_total:
        potential_savings = iopt_total - standard_total
        recommendation = f"Switch to Standard storage to save ${potential_savings:.2f}/month"
    else:
        recommendation = f"Current {current_storage_type} storage is optimal"
    
    return {
        'storage_gb': storage_gb,
        'backup_gb': backup_gb,
        'monthly_iops': monthly_iops,
        'actual_days': actual_days,
        'instance_count': len(instances),
        'standard_costs': {
            'compute': standard_compute_cost,
            'io': standard_io_cost,
            'storage': standard_storage_cost,
            'backup': backup_cost,
            'total': standard_total
        },
        'iopt_costs': {
            'compute': iopt_compute_cost,
            'io': iopt_io_cost,
            'storage': iopt_storage_cost,
            'backup': backup_cost,
            'total': iopt_total
        },
        'recommendation': recommendation,
        'potential_savings': potential_savings,
        'current_type': current_storage_type
    }