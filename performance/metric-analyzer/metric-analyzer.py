"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Description:
    This script analyzes the output of the Amazon DocumentDB Metric Collector
    Tool and provides recommendations for optimizing performance, cost, and 
    availability. 

Usage:
    python metric-analyzer.py --metrics-file <input-file-name> \\
        --region <aws-region-name> \\
        --output <output-file-name> \\
        --log-level <log-level> \\
        [--no-html]

Script Parameters
-----------------
--metrics-file: str (required)
    Path to the metrics CSV file to analyze
--region: str
    AWS Region (default: us-east-1)
--output: str
    Base name for output CSV file (default: metric-analyzer)
    The actual filename will include the current date (YYYY-MM-DD)
--log-level: str
    Log level for logging (choices: DEBUG, INFO, WARNING, ERROR, CRITICAL, default: WARNING)
--no-html: bool
    Disable HTML output generation (HTML output is enabled by default)
"""
import json
import argparse
import logging
import sys
from datetime import datetime
import boto3
import pandas as pd

# Recommendations
RECOMMENDATIONS = {
    'cpu_underutilized': {
        'category': 'Instance',
        'finding': 'Low CPU usage of %s',
        'recommendation': 'Consider decreasing the instance size',
        'reference': 'https://docs.aws.amazon.com/documentdb/latest/developerguide/best_practices.html#best_practices-performance',
        'context': 'context/cpu_underutilized.html'
    },
    'cpu_overutilized': {
        'category': 'Instance',
        'finding': 'High CPU usage of %s',
        'recommendation': 'Consider increasing the instance size',
        'reference': 'https://docs.aws.amazon.com/documentdb/latest/developerguide/best_practices.html#best_practices-performance',
        'context': 'context/cpu_overutilized.html'
    },
    'buffer_cache_low': {
        'category': 'Instance',
        'finding': 'Low BufferCacheHitRatio - %s',
        'recommendation': 'Analyze your cache performance and consider archiving unused data. Consider increasing instance size or utilizing NVMe-backed Instances',
        'reference': 'https://docs.aws.amazon.com/documentdb/latest/developerguide/best_practices.html#best_practices-instance_sizing',
        'context': 'context/buffer_cache_low.html'
    },
    'index_cache_low': {
        'category': 'Instance',
        'finding': 'Low IndexBufferCacheHitRatio - %s',
        'recommendation': 'Review and remove unused/redundant indexes or increase the instance size.',
        'reference': 'https://github.com/awslabs/amazon-documentdb-tools/tree/master/performance/index-review',
        'context': 'context/index_cache_low.html'
    },
    'read_preference': {
        'category': 'Cluster',
        'finding': 'Primary handling majority of OpcounterQuery (primary: %s; replica(s): %s)',
        'recommendation': 'Use secondaryPreferred driver read preference to maximize read scaling',
        'reference': 'https://docs.aws.amazon.com/documentdb/latest/developerguide/connect-to-replica-set.html',
        'context': 'context/read_preference.html'
    },
    'connection_limit': {
        'category': 'Instance',
        'finding': 'Connections (p99) approaching instance limit - %s of %s available',
        'recommendation': 'Consider increasing the instance size',
        'reference': 'https://docs.aws.amazon.com/documentdb/latest/developerguide/limits.html#limits.instance',
        'context': 'context/connection_limit.html'
    },
    'single_az': {
        'category': 'Cluster',
        'finding': 'Cluster deployed with a single instance - %s',
        'recommendation': 'Add replica instances to achieve higher availability and read scaling if this is a production workload.',
        'reference': 'https://docs.aws.amazon.com/documentdb/latest/developerguide/replication.html#replication.high-availability',
        'context': 'context/single_az.html'
    },
    'remove_instances': {
        'category': 'Cluster',
        'finding': 'Cluster deployed with more than 3 instances (%s)',
        'recommendation': 'Consider decreasing instance count - more than 3 instances does not improve availability',
        'reference': 'https://docs.aws.amazon.com/documentdb/latest/developerguide/replication.html#replication.high-availability',
        'context': 'context/remove_instances.html'
    },
    'graviton_upgrade': {
        'category': 'Instance',
        'finding': 'Using previous generation instance %s',
        'recommendation': 'Move to AWS Graviton2 instances which can provide 30% price/performance improvement and are 5% less expensive than their previous-generation counterparts',
        'reference': 'https://aws.amazon.com/blogs/database/achieve-better-performance-on-amazon-documentdb-with-aws-graviton2-instances/',
        'context': 'context/graviton_upgrade.html'
    }
}

# Metric thresholds
THRESHOLDS = {
    'cpu_low': 30.0,
    'cpu_high': 90.0,
    'cache_ratio_low': 90.0,
    'connection_limit_pct': 0.95
}

# Setup logger
def setup_logger(log_level=logging.INFO):
    logger = logging.getLogger('metric-analyzer')
    logger.setLevel(log_level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger

# Get latest instance values
def get_docdb_instance_specs(region_name):
    # Connection limits by instance type - unavailable via boto3, pulled from:
    # https://docs.aws.amazon.com/documentdb/latest/developerguide/limits.html#limits.instance
    
    connection_limits = {
        'db.r4.large': 1700,
        'db.r4.xlarge': 3400,
        'db.r4.2xlarge': 6800,
        'db.r4.4xlarge': 13600,
        'db.r4.8xlarge': 27200,
        'db.r4.16xlarge': 30000,
        'db.r6g.large': 3400,
        'db.r6g.xlarge': 7000,
        'db.r6g.2xlarge': 14200,
        'db.r6g.4xlarge': 28400,
        'db.r6g.8xlarge': 60000,
        'db.r6g.12xlarge': 60000,
        'db.r6g.16xlarge': 60000,
        'db.r5.large': 3400,
        'db.r5.xlarge': 7000,
        'db.r5.2xlarge': 14200,
        'db.r5.4xlarge': 28400,
        'db.r5.8xlarge': 60000,
        'db.r5.12xlarge': 60000,
        'db.r5.16xlarge': 60000,
        'db.r5.24xlarge': 60000,
        'db.r6gd.large': 3400,
        'db.r6gd.xlarge': 7000,
        'db.r6gd.2xlarge': 14200,
        'db.r6gd.4xlarge': 28400,
        'db.r6gd.8xlarge': 60000,
        'db.r6gd.12xlarge': 60000,
        'db.r6gd.16xlarge': 60000,
        'db.t4g.medium': 1000,
        'db.t3.medium': 1000
    }

    pricing_client = boto3.client('pricing')
    filters = [
        {'Type': 'TERM_MATCH', 'Field': 'productFamily', 'Value': 'Database Instance'},
        {'Type': 'TERM_MATCH', 'Field': 'servicecode', 'Value': 'AmazonDocDB'},
        {'Type': 'TERM_MATCH', 'Field': 'regionCode', 'Value': region_name}
    ]

    response = pricing_client.get_products(
        ServiceCode='AmazonDocDB',
        Filters=filters
    )
    
    if not response.get('PriceList'):
        raise ValueError(f"No pricing data found for Amazon DocumentDB in region: {region_name}")
    
    instance_data = []
    for price_item in response['PriceList']:
        price_dict = json.loads(price_item)
        required_fields = ['instanceType', 'volumeoptimization', 'networkPerformance', 'memory', 'vcpu']
        if 'product' not in price_dict or 'attributes' not in price_dict['product']:
            continue
            
        attributes = price_dict['product']['attributes']
        if any(field not in attributes for field in required_fields):
            continue
            
        instance_type = attributes['instanceType']
        
        # Price
        price_value = None
        if 'terms' in price_dict and 'OnDemand' in price_dict['terms']:
            for term_key in price_dict['terms']['OnDemand']:
                if 'priceDimensions' in price_dict['terms']['OnDemand'][term_key]:
                    for dim_key in price_dict['terms']['OnDemand'][term_key]['priceDimensions']:
                        dim = price_dict['terms']['OnDemand'][term_key]['priceDimensions'][dim_key]
                        if 'pricePerUnit' in dim and 'USD' in dim['pricePerUnit']:
                            currency = 'USD'
                            price_value = float(dim['pricePerUnit']['USD'])
                        elif 'pricePerUnit' in dim and 'CNY' in dim['pricePerUnit']:
                            price_value = float(dim['pricePerUnit']['CNY'])
                            currency = 'CNY'
                    if price_value:
                        break
        
        if not price_value:
            raise ValueError(f"Missing pricePerUnit for instance type {instance_type}")
        
        # RAM
        try:
            memory_value = float(attributes['memory'].split(' ')[0]) if 'GiB' in attributes['memory'] else None
            if memory_value is None:
                raise ValueError(f"Invalid memory format: {attributes['memory']}")
        except Exception as e:
            raise ValueError(f"Failed to parse memory for {instance_type}: {str(e)}")
            
        # vCPU
        try:
            vcpu_value = int(attributes['vcpu'])
        except Exception as e:
            raise ValueError(f"Failed to parse vcpu for {instance_type}: {str(e)}")
        
        # Connection limit
        if instance_type not in connection_limits:
            raise ValueError(f"Missing connection limit for instance type {instance_type}")
        
        # Build instance data - capturing additional fields for future use
        instance_data.append({
            'instance_type': instance_type,
            'region': region_name,
            'currency': currency,
            'standard_price_per_hour': price_value,
            'connections': connection_limits[instance_type],
            'Type': attributes['volumeoptimization'],
            'networkPerformance': attributes['networkPerformance'],
            'memory_GiB': memory_value,
            'vcpu': vcpu_value
        })
    
    if not instance_data:
        raise ValueError(f"Failed to extract any instance pricing data for Amazon DocumentDB in region: {region_name}")
        
    return pd.DataFrame(instance_data)

# Load metric-collector output file
def load_data(metrics_file):
    metrics_df = pd.read_csv(metrics_file)
    numeric_columns = ['P99', 'Mean', 'Std', 'Min', 'Max']
    for col in numeric_columns:
        if col in metrics_df.columns:
            if metrics_df[col].dtype == 'object':
                metrics_df[col] = metrics_df[col].str.replace(',', '').astype(float)
            else:
                metrics_df[col] = pd.to_numeric(metrics_df[col], errors='coerce')
    return metrics_df

# Get the metric data
def get_metric_data(instance_data, metric_name):
    metric_data = instance_data[instance_data['MetricName'] == metric_name]
    if metric_data.empty:
        return None
    
    return {
        'p99': float(metric_data['P99'].iloc[0]),
        'mean': float(metric_data['Mean'].iloc[0]),
        'std': float(metric_data['Std'].iloc[0])
        #'min': float(metric_data['Min'].iloc[0])
        #'max': float(metric_data['Max'].iloc[0])
    }

# Analyze CPU usage
def analyze_cpu_utilization(instance_data):
    logger = logging.getLogger('metric-analyzer')
    cpu_data = get_metric_data(instance_data, 'CPUUtilization')
    if not cpu_data:
        logger.info("No CPU data available. Skipping.")
        return "OK", ""
    
    p99 = cpu_data['p99']
    mean = cpu_data['mean']
    std = cpu_data['std']
    cpu_message = f"{p99:.1f}% (p99)"
    
    if p99 > THRESHOLDS['cpu_high'] or (mean + std) > 100:
        return "INCREASE", cpu_message
    elif p99 < THRESHOLDS['cpu_low']:
        return "DECREASE", cpu_message
    return "OK", ""

# Analyze cache hit ratios
def analyze_cache_ratio(instance_data):
    logger = logging.getLogger('metric-analyzer')
    results = {}
    cache_ratio_metrics = ['BufferCacheHitRatio', 'IndexBufferCacheHitRatio']
    
    for cache_metric in cache_ratio_metrics:
        cache_data = get_metric_data(instance_data, cache_metric)
        if not cache_data:
            logger.info(f"No {cache_metric} data available. Skipping.")
            continue
            
        p99 = cache_data['p99']
        mean = cache_data['mean']
        std = cache_data['std']
        mean_std = min(mean + std,100.00)

        if p99 < THRESHOLDS['cache_ratio_low'] or mean_std < THRESHOLDS['cache_ratio_low']:
            if p99 < THRESHOLDS['cache_ratio_low'] and mean_std < THRESHOLDS['cache_ratio_low']:
                if p99 <= mean_std:
                    cache_message = f"{p99:.1f}% (p99)"
                else:
                    cache_message = f"{mean_std:.1f}% (Mean+Std)"
            elif p99 < THRESHOLDS['cache_ratio_low']:
                cache_message = f"{p99:.1f}% (p99)"
            else: 
                cache_message = f"{mean_std:.1f}% (Mean+Std)"
                
            results[cache_metric] = ("INCREASE", cache_message)
        else:
            results[cache_metric] = ("OK", "")
    
    return results

# Analyze connection limits
def analyze_connections(instance_data, instance_specs_df):
    logger = logging.getLogger('metric-analyzer')
    instance_type = instance_data['InstanceType'].iloc[0]
    connection_data = get_metric_data(instance_data, 'DatabaseConnections')
    if not connection_data:
        logger.info("No connection data available. Skipping.")
        return "OK", ""
    
    p99 = connection_data['p99']
    instance_specs = instance_specs_df[instance_specs_df['instance_type'] == instance_data['InstanceType'].iloc[0]]
    if instance_specs.empty:
        logger.info(f"No specifications available for instance type {instance_type}. Skipping.")
        return "OK", ""
    
    max_connections = float(instance_specs['connections'].iloc[0])
    
    if p99 > (THRESHOLDS['connection_limit_pct'] * max_connections):
        return "INCREASE", (f"{p99:.1f}", f"{max_connections:.0f}")
    
    return "OK", ""

# Analyze current instance class for r4, r5, t3
def analyze_instance_type(instance_data):
    instance_type = instance_data['InstanceType'].iloc[0]
    if (instance_type.startswith('db.r4.') or 
        (instance_type.startswith('db.r5.') and not instance_type == 'db.r5.24xlarge') or
        instance_type.startswith('db.t3.')):
        return "UPGRADE", instance_type
    
    return "OK", ""

# Analyze MultiAZ
def analyze_multi_az(cluster_data):
    logger = logging.getLogger('metric-analyzer')
    multi_az_data = cluster_data[cluster_data['MetricName'] == 'MultiAZ']
    if multi_az_data.empty:
        logger.info("No MultiAZ data available. Skipping.")
        return "OK", ""
    
    if 'TRUE' in multi_az_data.values:
        valid_instances = cluster_data[cluster_data['InstanceName'] != '---']['InstanceName'].unique()
        instance_count = len(valid_instances)
        
        if instance_count > 3:
            return "DECREASE", str(instance_count)
        
        return "OK", ""
    
    else:
        instance_name = cluster_data[cluster_data['InstanceName'] != '---']['InstanceName'].iloc[0]
        return "INCREASE", instance_name

# Analyze read preference
def analyze_read_preference(cluster_data):
    logger = logging.getLogger('metric-analyzer')
    primary_data = cluster_data[cluster_data['Primary'] == True]
    secondary_data = cluster_data[cluster_data['Primary'] == False]
    if primary_data.empty or secondary_data.empty:
        logger.info("Missing primary or secondary data for read preference analysis. Skipping.")
        return "OK", ""
    
    metric = 'OpcountersQuery' 
    primary_metric = primary_data[primary_data['MetricName'] == metric]
    secondary_metric = secondary_data[secondary_data['MetricName'] == metric]
    if not primary_metric.empty and not secondary_metric.empty:
        primary_val = round(float(primary_metric['Mean'].iloc[0]))
        secondary_total = round(float(secondary_metric['Mean'].sum()))
        if primary_val > secondary_total:
            primary_int = str(int(primary_val))
            secondary_int = str(int(secondary_total))
            return "INCREASE", (primary_int, secondary_int)
    
    return "OK", ""

# Skip DECREASE recommendation for smallest instance types
def skip_recommendation(status, instance_type, rec_key=None):
    logger = logging.getLogger('metric-analyzer')
    if rec_key == 'graviton_upgrade':
        return False
        
    if status == "DECREASE" and instance_type.endswith('.medium'):
        logger.info(f"Skipping decrease recommendation for {instance_type}: already at smallest instance type")
        return True
    
    return False

# Create recommendation format
def add_recommendation(results, cluster_name, instance_name, instance_role, rec_key, details, instance_type='---', status=None):
    logger = logging.getLogger('metric-analyzer')
    try:
        if status and skip_recommendation(status, instance_type, rec_key):
            return
            
        rec = RECOMMENDATIONS[rec_key]
        finding = rec['finding']
        if '%s' in finding:
            finding = finding % details
        results.append({
            'ClusterName': cluster_name,
            'InstanceName': instance_name,
            'InstanceType': instance_type,
            'InstanceRole': instance_role,
            'Category': rec['category'],
            'ModifyInstance': status,
            'Finding': finding,
            'Recommendation': rec['recommendation'],
            'Reference': rec['reference'],
            'Context': rec.get('context', '')
        })
        logger.debug(f"Added recommendation: {rec_key} for {cluster_name}/{instance_name}")
    except KeyError:
        logger.error(f"Invalid recommendation key: {rec_key}")
        raise

# Generate interactive report with access to context files
def generate_html_report(results, output_file):
    """Generate an HTML report from the results."""
    html_output = f"{output_file}.html"
    context_contents = {}
    
    for result in results:
        if result.get('Context') and result['Context'] not in context_contents:
            context_path = result['Context']
            try:
                with open(context_path, 'r') as f:
                    context_contents[context_path] = f.read()
            except:
                context_contents[context_path] = "<p>Context file not found</p>"
    
    html = """<!DOCTYPE html>
    <html>
    <head>
        <title>DocumentDB Metric Analyzer Report</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            table { border-collapse: collapse; width: 100%; counter-reset: rowNumber; }
            table tr { counter-increment: rowNumber; }
            table tr td:first-child::before { content: counter(rowNumber); min-width: 1em; margin-right: 0.5em; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #f2f2f2; }
            tr:nth-child(even) { background-color: #f9f9f9; }
            .context-popup { display: none; position: fixed; top: 50%; left: 50%; 
                           transform: translate(-50%, -50%); background: white;
                           padding: 20px; border: 1px solid #ccc; box-shadow: 0 0 10px rgba(0,0,0,0.2);
                           max-width: 80%; max-height: 80%; overflow: auto; z-index: 1000; }
            .close-btn { float: right; cursor: pointer; font-weight: bold; }
            .overlay { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%;
                      background: rgba(0,0,0,0.5); z-index: 999; }
        </style>
        <script>
            // Store all context content in JavaScript
            const contextContents = {
    """
    
    for path, content in context_contents.items():
        escaped_content = content.replace('\\', '\\\\').replace("'", "\\'").replace('\n', '\\n')
        html += f"'{path}': '{escaped_content}',\n"
    
    html += """
            };
            
            function showContext(contextPath) {
                const content = contextContents[contextPath] || '<p>Context not available</p>';
                document.getElementById('context-content').innerHTML = content;
                document.getElementById('context-popup').style.display = 'block';
                document.getElementById('overlay').style.display = 'block';
            }
            
            function closeContext() {
                document.getElementById('context-popup').style.display = 'none';
                document.getElementById('overlay').style.display = 'none';
            }
        </script>
    </head>
    <body>
        <h1>DocumentDB Metric Analyzer Report</h1>
        <p>Generated on: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</p>
        <table>
            <tr>
                <th>#</th>
                <th>ClusterName</th>
                <th>InstanceName</th>
                <th>InstanceType</th>
                <th>InstanceRole</th>
                <th>Category</th>
                <th>ModifyInstance</th>
                <th>Finding</th>
                <th>Recommendation</th>
                <th>Reference</th>
                <th>Context</th>
            </tr>
    """
    
    # Add rows for each result
    for result in results:
        context_link = ""
        if result.get('Context'):
            context_path = result['Context']
            context_link = f'<a href="javascript:void(0)" onclick="showContext(\'{context_path}\')">View Context</a>'
        
        html += f"""
            <tr>
                <td></td>
                <td>{result['ClusterName']}</td>
                <td>{result['InstanceName']}</td>
                <td>{result['InstanceType']}</td>
                <td>{result['InstanceRole']}</td>
                <td>{result['Category']}</td>
                <td>{result['ModifyInstance']}</td>
                <td>{result['Finding']}</td>
                <td>{result['Recommendation']}</td>
                <td><a href="{result['Reference']}" target="_blank">AWS Docs</a></td>
                <td>{context_link}</td>
            </tr>
        """
    
    html += """
        </table>
        
        <div id="overlay" class="overlay" onclick="closeContext()"></div>
        <div id="context-popup" class="context-popup">
            <span class="close-btn" onclick="closeContext()">×</span>
            <div id="context-content"></div>
        </div>
    </body>
    </html>
    """
    
    with open(html_output, 'w') as f:
        f.write(html)
    return html_output

def main():
    parser = argparse.ArgumentParser(description='Analyze the output of metric-collector for Amazon DocumentDB clusters and provide recommendations.')
    
    parser.add_argument('--metrics-file',
                        type=str,
                        required=True,
                        help='Path to the metrics CSV file to analyze')
    
    parser.add_argument('--region',
                        type=str,
                        default='us-east-1',
                        help='AWS region name (default: us-east-1)')
    
    parser.add_argument('--output',
                        type=str,
                        default='metric-analyzer',
                        help='Path to output CSV file')

    parser.add_argument('--log-level',
                        type=str,
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='WARNING',
                        help='Set the logging level')
                        
    parser.add_argument('--no-html',
                        action='store_false',
                        dest='html_output',
                        help='Disable HTML output generation (enabled by default)')
    
    args = parser.parse_args()

    log_level = getattr(logging, args.log_level)
    logger = setup_logger(log_level)
    
    metrics_file = args.metrics_file
    region = args.region
    output_file = args.output
    
    logger.info(f"Starting analysis of metrics file: {metrics_file}")
    logger.info(f"Using AWS region: {region}")
    
    try:
        metrics_df = load_data(metrics_file)
        logger.info(f"Successfully loaded metrics data with {len(metrics_df)} rows")
        
        instance_specs_df = get_docdb_instance_specs(region)
        logger.info(f"Successfully loaded instance specifications for {len(instance_specs_df)} instance types")
    
        results = []
        
        clusters = metrics_df['ClusterName'].unique()
        logger.info(f"Found {len(clusters)} clusters to analyze")

        for cluster_name in clusters:
            logger.debug(f"Analyzing cluster: {cluster_name}")
            cluster_data = metrics_df[metrics_df['ClusterName'] == cluster_name]
        
            # Analyze read preference
            logger.debug(f"Analyzing read preference for cluster: {cluster_name}")
            read_pref_status, read_pref_values = analyze_read_preference(cluster_data)
            
            if read_pref_status != "OK":
                logger.info(f"Read preference finding for {cluster_name}: {read_pref_values}")
                add_recommendation(results, cluster_name, '---', '---', 'read_preference', read_pref_values)
        
            # Analyze MultiAZ
            logger.debug(f"Analyzing MultiAZ for cluster: {cluster_name}")
            multi_az_status, multi_az_message = analyze_multi_az(cluster_data)
            
            if multi_az_status != "OK":
                logger.info(f"MultiAZ finding for cluster {cluster_name}: {multi_az_message}")
                rec_key = 'single_az' if multi_az_status == 'INCREASE' else 'remove_instances'
                add_recommendation(results, cluster_name, '---', '---', rec_key, multi_az_message)

            # Analyze instance metrics
            instances = [i for i in cluster_data['InstanceName'].unique() if i != '---']
            logger.debug(f"Found {len(instances)} instances to analyze in cluster {cluster_name}")
            
            for instance_name in instances:
                logger.debug(f"Analyzing instance: {instance_name} in cluster {cluster_name}")
                instance_data = cluster_data[cluster_data['InstanceName'] == instance_name]
                is_primary = instance_data['Primary'].iloc[0] if not instance_data.empty else False
                instance_role = "PRIMARY" if is_primary else "SECONDARY"
                logger.debug(f"Instance {instance_name} role: {instance_role}")
                instance_type = instance_data['InstanceType'].iloc[0]
                
                # CPU utilization
                cpu_status, cpu_message = analyze_cpu_utilization(instance_data)
                if cpu_status != "OK":
                    logger.info(f"CPU utilization finding for {instance_name} in {cluster_name}: {cpu_message}")
                    rec_key = 'cpu_overutilized' if cpu_status == 'INCREASE' else 'cpu_underutilized'
                    add_recommendation(results, cluster_name, instance_name, instance_role, rec_key, cpu_message, instance_type, cpu_status)
                
                # Cache ratios
                cache_results = analyze_cache_ratio(instance_data)
                
                # Buffer cache
                buffer_status, buffer_message = cache_results['BufferCacheHitRatio']
                if buffer_status != "OK":
                    logger.info(f"Buffer cache finding for {instance_name} in {cluster_name}: {buffer_message}")
                    add_recommendation(results, cluster_name, instance_name, instance_role, 'buffer_cache_low', buffer_message, instance_type, buffer_status)
                
                # Index cache
                index_status, index_message = cache_results['IndexBufferCacheHitRatio']
                if index_status != "OK":
                    logger.info(f"Index cache finding for {instance_name} in {cluster_name}: {index_message}")
                    add_recommendation(results, cluster_name, instance_name, instance_role, 'index_cache_low', index_message, instance_type, index_status)
                
                # Connection limits
                conn_status, conn_message = analyze_connections(instance_data, instance_specs_df)
                if conn_status != "OK":
                    logger.info(f"Connection limit finding for {instance_name} in {cluster_name}: {conn_message}")
                    add_recommendation(results, cluster_name, instance_name, instance_role, 'connection_limit', conn_message, instance_type, conn_status)
                
                # Instance class
                instance_type_status, instance_type_message = analyze_instance_type(instance_data)
                if instance_type_status != "OK":
                    logger.info(f"Instance type finding for {instance_name} in {cluster_name}: {instance_type_message}")
                    add_recommendation(results, cluster_name, instance_name, instance_role, 'graviton_upgrade', instance_type_message, instance_type, instance_type_status)
    
        today = datetime.now().strftime('%Y-%m-%d')
        if results:
            csv_results = [{k: v for k, v in result.items() if k != 'Context'} for result in results]
            results_df = pd.DataFrame(csv_results)
            csv_output = f"{output_file}-{today}.csv"
            results_df.to_csv(csv_output, index=False)
            
            if args.html_output:
                html_output = generate_html_report(results, f"{output_file}-{today}")
                logger.info(f"HTML report saved to {html_output}")
    
            logger.info(f"Analysis complete. Found {len(results)} recommendations.")
            logger.info(f"Results saved to {csv_output}")

        else:
            logger.info("Analysis complete. No recommendations found.")
    
    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.getLogger('metric-analyzer').critical(f"Unhandled exception: {str(e)}", exc_info=True)
        sys.exit(1)