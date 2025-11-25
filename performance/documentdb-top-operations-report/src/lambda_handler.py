from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.logging.formatter import LambdaPowertoolsFormatter
from datetime import datetime, timedelta
import json
import os
from typing import Dict

from docdb_profiler_analyzer import DocDBProfilerAnalyzer, analyze_profiler_logs

# Initialize Powertools
formatter = LambdaPowertoolsFormatter(utc=True, log_record_order=["message"])
logger = Logger(service="DocDBProfilerService", logger_formatter=formatter)
tracer = Tracer(service="DocDBProfilerService")
metrics = Metrics(namespace="DocDBProfilerMetrics")


@logger.inject_lambda_context
@tracer.capture_lambda_handler
@metrics.log_metrics
def lambda_handler(event: Dict, context: LambdaContext) -> Dict:
    """Main Lambda handler with full Powertools instrumentation."""
    try:
        # Validate required environment variables
        if os.getenv("DOCDB_LOG_GROUP_NAME") is None:
            raise ValueError("DOCDB_LOG_GROUP_NAME environment variable is not set")
        
        if os.getenv("SENDER_EMAIL") is None:
            raise ValueError("SENDER_EMAIL environment variable is not set")
            
        if os.getenv("RECIPIENT_EMAIL_LIST") is None:
            raise ValueError("RECIPIENT_EMAIL_LIST environment variable is not set")
        
        # Read environment variables
        log_groups_str = os.getenv("DOCDB_LOG_GROUP_NAME")
        
        # Handle TOP_OPS_COUNT with error handling for invalid values
        top_ops_count = None
        if (value := os.getenv("TOP_OPS_COUNT")) and value.strip():
            try:
                top_ops_count = int(value)
            except ValueError:
                logger.warning(f"Invalid TOP_OPS_COUNT value '{value}', using None (all operations)")
                top_ops_count = None
        
        sender_email = os.getenv("SENDER_EMAIL")
        recipient_emails = os.getenv("RECIPIENT_EMAIL_LIST")
        
        # Parse time range from environment variables
        start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S") if (start_time_str := os.getenv("REPORT_START_TIME")) else None
        end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S") if (end_time_str := os.getenv("REPORT_END_TIME")) else None
        
        # Call analysis function
        results = analyze_profiler_logs(
            log_groups_str=log_groups_str,
            top_ops_count=top_ops_count,
            start_time=start_time,
            end_time=end_time,
            sender_email=sender_email,
            recipient_emails=recipient_emails
        )
        
        # Process results and send emails
        if not results:
            logger.info("No profiler logs found")
            return {
                "statusCode": 200,
                "body": "No profiler logs found"
            }
        
        for result in results:
            # Prepare response for email
            email_response = {
                "top_operations": result['top_operations'].to_dict(orient='records'),
                "time_range": {
                    "start": result['time_range']['start'].isoformat(),
                    "end": result['time_range']['end'].isoformat()
                }
            }
            
            logger.info("Successfully analyzed profiler logs", extra={
                "cluster_name": result['cluster_name'],
                "total_ops": len(result['top_operations']),
            })
            
            # Send output via email using the analyzer instance
            result['analyzer'].send_response_via_email(email_response)
        
        return {
            "statusCode": 200,
            "body": "Top operations report published."
        }
        
    except Exception as error:
        logger.exception("Error processing profiler logs")
        metrics.add_metric(name="ProcessingErrors", unit=MetricUnit.Count, value=1)
        
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(error)})
        }