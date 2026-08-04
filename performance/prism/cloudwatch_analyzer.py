import boto3
from datetime import datetime, timedelta
import statistics
from functools import lru_cache


@lru_cache(maxsize=16)
def get_cpu_metrics(cluster_id, region='us-east-1', hours=48):
    """Get CPU utilization metrics for all DocumentDB instances in cluster"""
    try:
        docdb = boto3.client('docdb', region_name=region)
        cloudwatch = boto3.client('cloudwatch', region_name=region)

        cluster_response = docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)
        if not cluster_response['DBClusters']:
            return None

        cluster = cluster_response['DBClusters'][0]
        instance_ids = [member['DBInstanceIdentifier'] for member in cluster['DBClusterMembers']]
        if not instance_ids:
            return None

        instances_response = docdb.describe_db_instances()
        instance_types = {}
        for instance in instances_response['DBInstances']:
            if instance['DBInstanceIdentifier'] in instance_ids:
                instance_types[instance['DBInstanceIdentifier']] = instance['DBInstanceClass']

        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)

        all_instances_metrics = {}

        for instance_id in instance_ids:
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/DocDB',
                MetricName='CPUUtilization',
                Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Average', 'Maximum', 'Minimum']
            )

            if response['Datapoints']:
                datapoints = sorted(response['Datapoints'], key=lambda x: x['Timestamp'])
                cpu_values = [dp['Average'] for dp in datapoints]
                cpu_max_values = [dp['Maximum'] for dp in datapoints]

                cpu_avg = statistics.mean(cpu_values)
                cpu_max = max(cpu_max_values)
                cpu_min = min(cpu_values)
                cpu_std = statistics.stdev(cpu_values) if len(cpu_values) > 1 else 0
                cpu_cv = (cpu_std / cpu_avg * 100) if cpu_avg > 0 else 0

                idle_periods = sum(1 for cpu in cpu_values if cpu < 10)
                idle_percentage = (idle_periods / len(cpu_values)) * 100
                peak_periods = sum(1 for cpu in cpu_values if cpu > 70)
                peak_percentage = (peak_periods / len(cpu_values)) * 100

                all_instances_metrics[instance_id] = {
                    'cpu_avg': round(cpu_avg, 2),
                    'cpu_max': round(cpu_max, 2),
                    'cpu_min': round(cpu_min, 2),
                    'cpu_std': round(cpu_std, 2),
                    'cpu_cv': round(cpu_cv, 2),
                    'idle_percentage': round(idle_percentage, 2),
                    'peak_percentage': round(peak_percentage, 2),
                    'total_samples': len(cpu_values),
                    'datapoints': cpu_values[-48:] if len(cpu_values) > 48 else cpu_values,
                    'instance_type': instance_types.get(instance_id, 'db.r6g.xlarge')
                }

        if all_instances_metrics:
            cluster_avg = statistics.mean([m['cpu_avg'] for m in all_instances_metrics.values()])
            cluster_max = max([m['cpu_max'] for m in all_instances_metrics.values()])
            cluster_cv = statistics.mean([m['cpu_cv'] for m in all_instances_metrics.values()])
            cluster_idle = statistics.mean([m['idle_percentage'] for m in all_instances_metrics.values()])
            cluster_peak = statistics.mean([m['peak_percentage'] for m in all_instances_metrics.values()])

            return {
                'cluster_summary': {
                    'cpu_avg': round(cluster_avg, 2),
                    'cpu_max': round(cluster_max, 2),
                    'cpu_cv': round(cluster_cv, 2),
                    'idle_percentage': round(cluster_idle, 2),
                    'peak_percentage': round(cluster_peak, 2),
                    'instance_count': len(all_instances_metrics)
                },
                'instances': all_instances_metrics
            }

        return None

    except Exception:
        return None
