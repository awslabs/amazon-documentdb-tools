from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.logging.formatter import LambdaPowertoolsFormatter
import pandas as pd
import boto3
from datetime import datetime, timedelta
import json
from typing import List, Dict
import time
import os
import argparse

from cloudwatch_query import CloudWatchQuery
from date_utilities import DateUtilities

# Initialize Powertools
formatter = LambdaPowertoolsFormatter(utc=True, log_record_order=["message"])
logger = Logger(service="DocDBProfilerService",logger_formatter=formatter)
tracer = Tracer(service="DocDBProfilerService")
metrics = Metrics(namespace="DocDBProfilerMetrics")

class DocDBProfilerAnalyzer:
    @tracer.capture_method
    def __init__(self, log_group: Dict, top_ops_count: int = None, sender_email: str = None, recipient_emails: str = None):
        """Initialize the DocDB Profiler Analyzer """
        self.cluster_name = log_group.get("cluster_name")
        self.log_group_name = log_group.get("profiler_log")         
        self.top_ops_count = top_ops_count
        session = boto3.Session()
        self.region = session.region_name
        self.sender_email_address = sender_email
        self.recipient_email_list = [email.strip() for email in recipient_emails.split(',')] if recipient_emails else []
        self.logs_client = boto3.client('logs', region_name=self.region)
        logger.info(f"Initialized DocDBProfilerAnalyzer for log group: {self.log_group_name}")

    @tracer.capture_method
    def fetch_profiler_logs(self, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """Fetch profiler logs using CloudWatchQuery for handling large result sets (>10,000 results)."""
        try:
            query = """
            fields @timestamp, @message
            | sort @timestamp asc
            """
            
            # Initialize date utilities for conversion
            date_utilities = DateUtilities()
            
            # Convert datetime objects to ISO8601 format for CloudWatchQuery
            start_time_iso8601 = date_utilities.convert_datetime_to_iso8601(start_time)
            end_time_iso8601 = date_utilities.convert_datetime_to_iso8601(end_time)
            
            logger.info(f"Starting CloudWatch query for log group: {self.log_group_name}")
            logger.info(f"Time range: {start_time_iso8601} to {end_time_iso8601}")
            
            with tracer.provider.in_subsegment("start_logs_query") as subsegment:
                subsegment.put_annotation("log_group", self.log_group_name)
                subsegment.put_annotation("start_time", start_time_iso8601)
                subsegment.put_annotation("end_time", end_time_iso8601)
                
                # Use CloudWatchQuery for recursive querying to handle large result sets
                cloudwatch_query = CloudWatchQuery(
                    log_group=self.log_group_name,
                    query_string=query
                )
                
                # Execute the query with date range tuple
                cloudwatch_query.query_logs((start_time_iso8601, end_time_iso8601))
                
                # Get all results from the recursive query
                log_entries = cloudwatch_query.query_results
                
                logger.info(f"CloudWatch query completed in {cloudwatch_query.query_duration} seconds")
                logger.info(f"Total log entries retrieved: {len(log_entries)}")
            
            # Parse results into DataFrame
            df = self._parse_logs_to_dataframe(log_entries)
            
            # Record metrics
            metrics.add_metric(name="LogsProcessed", unit=MetricUnit.Count, value=len(df))
            metrics.add_metric(name="QueryDuration", unit=MetricUnit.Seconds, value=cloudwatch_query.query_duration)
            
            logger.info(f"Successfully processed {len(df)} log entries into DataFrame")
            return df
            
        except Exception as error:
            logger.exception("Error fetching profiler logs with CloudWatchQuery")
            metrics.add_metric(name="LogFetchErrors", unit=MetricUnit.Count, value=1)
            raise

    @tracer.capture_method
    def _parse_logs_to_dataframe(self, log_entries: List[Dict]) -> pd.DataFrame:
        """Parse log entries into DataFrame with error handling."""
        parsed_entries = []
        with tracer.provider.in_subsegment("parse_logs_to_dataframe"):
            for entry in log_entries:
                try:
                    message = json.loads(entry[1]["value"])
                    message['timestamp'] = entry[0]["value"]
                    parsed_entries.append(message)
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse log entry: {entry}")
                    metrics.add_metric(name="LogParseErrors", unit=MetricUnit.Count, value=1)
                
        return pd.DataFrame(parsed_entries)
    
    @tracer.capture_method
    def _process_filter(self,df: pd.DataFrame) -> pd.DataFrame:
        """Process filter field in DataFrame by removing actual filter value"""
        try:
            def get_filter_field(row):
                if row["op"] == "query":
                    return "filter"
                elif row["op"] == "command":
                    return "pipeline"
                else:
                    return "q"
            try:
                logger.info(f"Processing filters for {len(df)} operations")
                logger.info(f"Operations by type: {df['op'].value_counts().to_dict()}")
                
                def safe_get_filter(row):
                    try:
                        if 'command' not in row or pd.isna(row['command']):
                            return {}
                        return row["command"].get(get_filter_field(row), {})
                    except Exception as e:
                        logger.warning(f"Error processing filter for row: {e}")
                        return {}
                
                filters = df.apply(safe_get_filter, axis=1)
                logger.info(f"Successfully processed filters for {len(filters)} operations")
            except KeyError as ke:
                logger.exception(f"Missing required column: {ke}")
                metrics.add_metric(name="ProcessFilterErrors", unit=MetricUnit.Count, value=1)
                raise 
            except AttributeError as ae:
                logger.exception(f"Missing required column: {ae}")
                metrics.add_metric(name="ProcessFilterErrors", unit=MetricUnit.Count, value=1)
                raise
            
            def replace_values(d):
                try:
                    def process_value(value, value_map, counter):
                        """Recursively process values, preserving operator structure"""
                        if isinstance(value, dict):
                            # For dictionaries, process each key-value pair
                            new_dict = {}
                            for k, v in value.items():
                                if k.startswith('$'):
                                    # preserve structure, process the value
                                    new_dict[k], counter = process_value(v, value_map, counter)
                                else:
                                    # Regular field - process the value
                                    new_dict[k], counter = process_value(v, value_map, counter)
                            return new_dict, counter
                        elif isinstance(value, list):
                            # For lists, process each item
                            new_list = []
                            for item in value:
                                processed_item, counter = process_value(item, value_map, counter)
                                new_list.append(processed_item)
                            return new_list, counter
                        else:
                            # Leaf value - replace with placeholder
                            value_key = str(value)
                            if value_key not in value_map:
                                value_map[value_key] = f"V{counter[0]}"
                                counter[0] += 1
                            return value_map[value_key], counter
                    
                    def process_dict(d, op):
                        value_map = {}  
                        counter = [1]  # Use list to make it mutable
                        new_d = {}
                
                        for key, value in d.items():
                            if (op == 'pipeline' and key == "$match"):
                                # Process $match stage - preserve operators
                                processed_match, _ = process_value(value, value_map, counter)
                                new_d = {"$match": processed_match}
                            elif op == 'pipeline':
                                new_d[key] = value
                            else:
                                # For query operations, process the value
                                processed_value, _ = process_value(value, value_map, counter)
                                new_d[key] = processed_value
                        return new_d
                    if isinstance(d, list):
                        return [process_dict(item,'pipeline') for item in d if isinstance(item, dict)]
                    elif isinstance(d, dict):
                        return process_dict(d, 'query')
                    return d
                except (AttributeError, TypeError) as e:
                    logger.exception(f"Invalid filter format: {str(e)}")
                    metrics.add_metric(name="ProcessFilterErrors", unit=MetricUnit.Count, value=1)
                    raise
            df = df.copy()  
            df["modified_filter"] = filters.apply(replace_values)
            return df
        except Exception as e:
            logger.exception(f"Invalid filter format: {str(e)}")
            metrics.add_metric(name="ProcessFilterErrors", unit=MetricUnit.Count, value=1)
            raise

    @tracer.capture_method
    def _process_exec_stats(self,df: pd.DataFrame) -> pd.DataFrame:
        """Process executionStats field in DataFrame by removing nReturned and executionTimeMillisEstimate value"""
        try:
            logger.info(f"Processing execStats for {len(df)} operations")
            
            def replace_value(d):
                try:
                    if isinstance(d, dict):
                        for key in ["nReturned", "executionTimeMillisEstimate"]:
                            if key in d:
                                d[key] = "?"  # Replace value with '?'
                        
                        for k, v in d.items():  # Recursively process nested structures
                            d[k] = replace_value(v)
                    
                    elif isinstance(d, list):
                        return [replace_value(item) for item in d]
                    return d
                except Exception as e:
                    logger.exception(f"Invalid execStats format: {str(e)}")
                    metrics.add_metric(name="ProcessExecStatsErrors", unit=MetricUnit.Count, value=1)
                    raise
                
            df = df.copy()
            
            # Handle missing execStats field safely
            if 'execStats' not in df.columns:
                logger.info("execStats column doesn't exist, creating with empty dict")
                df['execStats'] = [{}] * len(df)
            else:
                logger.info(f"execStats column exists, null count: {df['execStats'].isnull().sum()}")
                df['execStats'] = df['execStats'].fillna({})
            
            df["modified_execStats"] = df["execStats"].apply(replace_value)
            logger.info(f"Successfully processed execStats for {len(df)} operations")
            return df 
        except Exception as e:
            logger.exception(f"Error processing execStats: {str(e)}")
            metrics.add_metric(name="ProcessExecStatsErrors", unit=MetricUnit.Count, value=1)
            raise
    
    @tracer.capture_method
    def _get_agg_dict(self, df: pd.DataFrame) -> Dict:
        """
        Create aggregation dictionary based on available columns in dataframe
        """
        # Base aggregation dictionary with required fields
        agg_dict = {
            'millis': ['mean', 'max', 'count']
        }
        
        # Optional fields - only add if they exist in the dataframe
        optional_fields = ['nreturned','nInserted', 'nModified', 'nRemoved']
        
        for field in optional_fields:
            if field in df.columns:
                agg_dict[field] = 'mean'
        
        return agg_dict

    @tracer.capture_method
    def get_top_operations(self, df: pd.DataFrame, sort_by: str = 'millis') -> pd.DataFrame:
        """Extract top N slowest operations with metrics tracking."""
        df = self._process_filter(df)
        df = self._process_exec_stats(df)
        df["modified_filter_str"] = df["modified_filter"].apply(lambda x: str(x))  # Convert dict to string
        df["modified_execStats_str"] = df["modified_execStats"].apply(lambda x: str(x))  # Convert dict to string
        
        # Always use appName - fill with "N/A" if it doesn't exist or has missing values
        if 'appName' not in df.columns:
            logger.info("appName column doesn't exist, creating with N/A")
            df['appName'] = 'N/A'
        else:
            logger.info(f"appName column exists, null count: {df['appName'].isnull().sum()}")
            df['appName'] = df['appName'].fillna('N/A')
        
        
        # Get dynamic aggregation dictionary based on available columns
        agg_dict = self._get_agg_dict(df)
                
        top_ops = df.groupby(['op', 'ns', 'user', 'appName', 'modified_filter_str', 'modified_execStats_str']).agg(
            agg_dict
        ).reset_index()
                

        # Create list of column names based on available aggregations
        columns = ['operation', 'namespace', 'user', 'appName', 'filter_criterion', 'execution_stats', 
                    'avg_duration_ms', 'max_duration_ms', 'count']
        
        # Add optional columns if they exist
        if 'nreturned' in agg_dict:
            columns.append('avg_docs_returned')
        if 'nInserted' in agg_dict:
            columns.append('avg_docs_inserted')
        if 'nModified' in agg_dict:
            columns.append('avg_docs_modified')
        if 'nRemoved' in agg_dict:
            columns.append('avg_docs_removed')

        top_ops.columns = columns
        # Convert duration columns to int
        columns_to_convert = [x for x in columns if x not in ['operation','namespace','user', 'appName','filter_criterion','execution_stats']]
        for col in columns_to_convert:
            top_ops[col] = top_ops[col].fillna(0).astype(int)

        sort_column = 'avg_duration_ms' if sort_by == 'millis' else sort_by

        if self.top_ops_count:
            result = top_ops.sort_values(sort_column, ascending=False).head(self.top_ops_count)
        else:
            result = top_ops.sort_values(sort_column, ascending=False)

        # Record metrics for monitoring
        metrics.add_metric(
            name="MaxOperationDuration",
            unit=MetricUnit.Milliseconds,
            value=float(result['max_duration_ms'].max())
        )
        
        return result
        
    @tracer.capture_method
    def _format_response_to_html(self,data: Dict):
        """Format data(Dict) into html table"""
        time_range = f"{data['time_range']['start']} to {data['time_range']['end']}"
        
        table_rows = "".join(
            f"""
            <tr>
                <td>{op['operation']}</td>
                <td>{op['namespace']}</td>
                <td>{op['user']}</td>
                <td>{op['appName']}</td>
                <td>{op['filter_criterion']}</td>
                <td>{op['execution_stats']}</td>
                <td>{op['avg_duration_ms']}</td>
                <td>{op['max_duration_ms']}</td>
                <td>{op['count']}</td>
                <td>{op.get('avg_docs_returned', 'N/A')}</td>
                <td>{op.get('avg_docs_inserted', 'N/A')}</td>
                <td>{op.get('avg_docs_modified', 'N/A')}</td>
                <td>{op.get('avg_docs_removed', 'N/A')}</td>
            </tr>
            """
            for op in data['top_operations']
        )
        
        html_message = f"""
        <html>
        <body>
            <p>Here are the top{' ' + str(self.top_ops_count) if self.top_ops_count is not None else ''} operations from the Document DB cluster <b>{self.cluster_name}</b> for the time range: {time_range}</p>
            <table border="1" cellpadding="5" cellspacing="0">
                <tr>
                    <th>Operation</th>
                    <th>Namespace</th>
                    <th>User</th>
                    <th>Application Name</th>
                    <th>Operation Pattern</th>
                    <th>Execution Stats</th>
                    <th>Avg Duration (ms)</th>
                    <th>Max Duration (ms)</th>
                    <th>Count</th>
                    <th>Avg Docs Returned</th>
                    <th>Avg Docs Inserted</th>
                    <th>Avg Docs Modified</th>
                    <th>Avg Docs Removed</th>
                </tr>
                {table_rows}
            </table>
        </body>
        </html>
        """
        
        return html_message

    @tracer.capture_method
    def send_response_via_email(self,response:Dict) -> Dict:
        """Send response via email"""
        if not self.sender_email_address or not self.recipient_email_list:
            raise ValueError("Email configuration missing. Set SENDER_EMAIL and RECIPIENT_EMAIL_LIST environment variables.")

        
        message = self._format_response_to_html(response)
        ses_client = boto3.client("ses")
        email_response = ses_client.send_email(
            Source=self.sender_email_address,
            Destination={"ToAddresses": self.recipient_email_list},
            Message={
                "Subject": {"Data": f"DocumentDB Top Operations Report for cluster {self.cluster_name}"},
                "Body": {"Html": {"Data": message}}
            }
        )
        logger.info(f"Email sent to {self.recipient_email_list}")
        return email_response

    @tracer.capture_method
    @staticmethod
    def parse_log_groups(log_groups_str: str) -> list:
        """
        Parse comma-separated log group string into a list of dictionaries
        containing cluster name and profile log path
        """
        try:
            # Split the string by comma and strip whitespace
            log_groups = [lg.strip() for lg in log_groups_str.split(',')]
            
            result = []
            for log_group in log_groups:
                # Split the path and extract cluster name
                parts = log_group.split('/')
                if len(parts) >= 4:
                    cluster_name = parts[3]  # Get the cluster name part
                    result.append({
                        "cluster_name": cluster_name,
                        "profiler_log": log_group
                    })
                
            return result
        except Exception as e:
            logger.exception(f"Error parsing log groups: {str(e)}")
            raise



def analyze_profiler_logs(log_groups_str: str, top_ops_count: int = None, 
                         start_time: datetime = None, end_time: datetime = None,
                         sender_email: str = None, recipient_emails: str = None) -> List[Dict]:
    """
    Common function to analyze profiler logs that can be used by both CLI and Lambda.
    
    Args:
        log_groups_str: Comma-separated list of DocumentDB profiler log groups
        top_ops_count: Number of top operations to return
        start_time: Report start time (defaults to 24 hours ago)
        end_time: Report end time (defaults to now)
        sender_email: Sender email for SES (optional)
        recipient_emails: Comma-separated recipient emails (optional)
    
    Returns:
        List of dictionaries containing analysis results for each log group
    """
    try:
        # Parse log groups
        log_groups = DocDBProfilerAnalyzer.parse_log_groups(log_groups_str)
        
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=1)
        if end_time is None:
            end_time = datetime.now()
        
        results = []
        
        for log_group in log_groups:
            logger.info(f"Processing log group: {log_group['cluster_name']}")
            
            # Initialize analyzer
            analyzer = DocDBProfilerAnalyzer(log_group, top_ops_count, sender_email, recipient_emails)
            
            # Fetch and analyze logs
            df = analyzer.fetch_profiler_logs(start_time, end_time)
            
            # Skip if no logs found
            if df.empty:
                logger.info(f"No profiler logs found for log group: {log_group['cluster_name']}")
                continue
            
            # Get top operations
            top_ops = analyzer.get_top_operations(df)
            
            # Prepare result
            result = {
                "cluster_name": log_group['cluster_name'],
                "log_group": log_group['profiler_log'],
                "top_operations": top_ops,
                "time_range": {
                    "start": start_time,
                    "end": end_time
                },
                "analyzer": analyzer  # Include analyzer for email functionality
            }
            
            results.append(result)
            logger.info(f"Found {len(top_ops)} top operations for {log_group['cluster_name']}")
        
        return results
        
    except Exception as error:
        logger.exception("Error in analyze_profiler_logs")
        raise


def main():
    """Main function for command line execution."""
    parser = argparse.ArgumentParser(description='DocumentDB Profiler Analyzer')
    
    # Required arguments
    parser.add_argument('--log-groups', 
                       required=True,
                       help='Comma-separated list of DocumentDB profiler log groups')
    
    # Optional arguments
    parser.add_argument('--top-ops-count', 
                       type=int,
                       help='Number of top operations to return')
    
    parser.add_argument('--output-dir',
                       default='.',
                       help='Output directory for CSV files (default: current directory)')
    
    parser.add_argument('--start-time',
                       help='Report start time in format "YYYY-MM-DD HH:MM:SS"')
    
    parser.add_argument('--end-time',
                       help='Report end time in format "YYYY-MM-DD HH:MM:SS"')
    
    args = parser.parse_args()
    
    try:
        # Parse time arguments
        start_time = datetime.strptime(args.start_time, "%Y-%m-%d %H:%M:%S") if args.start_time else None
        end_time = datetime.strptime(args.end_time, "%Y-%m-%d %H:%M:%S") if args.end_time else None
        
        # Create output directory if it doesn't exist
        os.makedirs(args.output_dir, exist_ok=True)
        
        # Call analysis function
        results = analyze_profiler_logs(
            log_groups_str=args.log_groups,
            top_ops_count=args.top_ops_count,
            start_time=start_time,
            end_time=end_time
        )
        
        # Process results and create CSV files
        for result in results:
            cluster_name = result['cluster_name']
            top_ops = result['top_operations']
            time_range = result['time_range']
            
            print(f"Processing log group: {cluster_name}")
            print(f"Analyzing logs from {time_range['start']} to {time_range['end']}")
            print(f"Found {len(top_ops)} top operations")
            
            # Create CSV filename with cluster name and report time range
            start_str = time_range['start'].strftime("%Y%m%d_%H%M%S")
            end_str = time_range['end'].strftime("%Y%m%d_%H%M%S")
            csv_filename = f"docdb_top_operations_{cluster_name}_{start_str}_to_{end_str}.csv"
            csv_path = os.path.join(args.output_dir, csv_filename)
            
            # Save to CSV file
            top_ops.to_csv(csv_path, index=False)
            print(f"Report saved to: {csv_path}")
        
        if not results:
            print("No profiler logs found for any log groups")
        else:
            print("Analysis completed successfully!")
        
    except Exception as error:
        print(f"Error processing profiler logs: {str(error)}")
        raise


if __name__ == "__main__":
    main()