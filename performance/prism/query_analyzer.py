import boto3
import json
import re
import time
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# All DocumentDB profiler op types
_VALID_OPS = {'query', 'update', 'command', 'insert', 'delete', 'getMore', 'aggregate',
              'find', 'remove', 'count', 'distinct', 'findAndModify'}


def _sanitize_extended_json(text):
    """Remove MongoDB extended JSON constructs that break json.loads."""
    text = re.sub(r"ISODate\(['\"]([^'\"]*)['\"]\)", r'"\1"', text)
    text = re.sub(r"NumberLong\(([^)]*)\)", r'\1', text)
    text = re.sub(r"NumberInt\(([^)]*)\)", r'\1', text)
    text = re.sub(r"NumberDecimal\(['\"]([^'\"]*)['\"]\)", r'"\1"', text)
    text = re.sub(r"ObjectId\(['\"]([^'\"]*)['\"]\)", r'"\1"', text)
    text = re.sub(r"Timestamp\(([^)]+)\)", r'\1', text)
    text = re.sub(r"BinData\([^)]+\)", r'"<BinData>"', text)
    return text

def get_query_patterns(database, collection, hours=24, log_group_name=None, aws_region='us-east-1'):
    """Get and analyze slow queries from CloudWatch"""
    try:
        if not log_group_name:
            logger.warning("No log group name provided")
            return []
        
        cloudwatch = boto3.client('logs', region_name=aws_region)
        
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        # Filter to only include read/update/delete/aggregate ops (exclude inserts which are write-heavy noise)
        op_filter = '| filter @message like /"op":"command"/ or @message like /"op":"query"/ or @message like /"op":"update"/ or @message like /"op":"delete"/ or @message like /"op":"count"/'

        # Sanitize database/collection names to prevent CloudWatch Insights injection
        def _safe_name(name):
            """Strip characters that could inject query logic."""
            return re.sub(r'["|\\`\n\r]', '', name)

        if database == "*" and collection == "*":
            # Cluster-wide: no namespace filter
            query_string = f'''
            fields @timestamp, @message
            {op_filter}
            | sort @timestamp desc
            | limit 5000
            '''
        elif collection == "*":
            namespace_filter = f"{_safe_name(database)}."
            query_string = f'''
            fields @timestamp, @message
            | filter @message like "{namespace_filter}"
            {op_filter}
            | sort @timestamp desc
            | limit 2000
            '''
        else:
            namespace_filter = f"{_safe_name(database)}.{_safe_name(collection)}"
            query_string = f'''
            fields @timestamp, @message
            | filter @message like "{namespace_filter}"
            {op_filter}
            | sort @timestamp desc
            | limit 2000
            '''
        
        logger.info("CloudWatch query: log_group=%s, start=%s, end=%s, hours=%d",
                    log_group_name, start_time.isoformat(), end_time.isoformat(), hours)
        
        start_response = cloudwatch.start_query(
            logGroupName=log_group_name,
            startTime=int(start_time.timestamp()),
            endTime=int(end_time.timestamp()),
            queryString=query_string.strip()
        )
        
        query_id = start_response['queryId']
        logger.info("CloudWatch query started: %s", query_id)
        
        # Wait for query completion
        max_wait_time = 60
        wait_interval = 2
        elapsed_time = 0
        
        while elapsed_time < max_wait_time:
            result_response = cloudwatch.get_query_results(queryId=query_id)
            status = result_response['status']
            
            if status == 'Complete':
                break
            elif status == 'Failed':
                return []
            
            time.sleep(wait_interval)
            elapsed_time += wait_interval
        
        if elapsed_time >= max_wait_time:
            logger.warning("CloudWatch query timed out after %ds", max_wait_time)
            return []
        
        results = result_response.get('results', [])
        logger.info("CloudWatch query returned %d raw results", len(results))
        pattern_groups = {}
        
        for result in results:
            try:
                message_field = next((field for field in result if field['field'] == '@message'), None)
                if not message_field:
                    logger.warning("No @message field in result: %s", [f['field'] for f in result])
                    continue
                
                log_data = json.loads(_sanitize_extended_json(message_field['value']))
                
                duration = log_data.get('millis', log_data.get('durationMillis', 0))
                operation = log_data.get('op', log_data.get('type', ''))
                command = log_data.get('command', log_data.get('query', {}))
                if not isinstance(command, dict):
                    command = {}

                logger.info("Parsed log: op=%s, millis=%s, ns=%s", operation, duration, log_data.get('ns', '?'))

                # Drop only sub-1ms noise and internal system ops; keep everything else
                if duration < 1:
                    logger.info("Filtered out (sub-1ms): duration=%s, op=%s", duration, operation)
                    continue
                if operation and operation not in _VALID_OPS and operation.startswith('$'):
                    logger.info("Filtered out (internal op): op=%s", operation)
                    continue
                
                pattern_key = create_query_pattern_key(command, operation)
                
                if pattern_key not in pattern_groups:
                    pattern_groups[pattern_key] = {
                        'operation': operation,
                        'durations': [],
                        'example_query': command,
                        'ns': log_data.get('ns', ''),
                        'count': 0
                    }
                
                pattern_groups[pattern_key]['durations'].append(duration)
                pattern_groups[pattern_key]['count'] += 1
                
            except json.JSONDecodeError as e:
                logger.warning("JSON parse failed: %s — raw: %.120s", e, message_field.get('value', ''))
                continue
            except Exception as e:
                logger.warning("Log entry processing failed: %s", e)
                continue
        
        query_patterns = []
        for pattern_key, group in pattern_groups.items():
            durations = group['durations']
            if durations:
                avg_time = sum(durations) / len(durations)
                max_time = max(durations)
                
                query_patterns.append({
                    'pattern_key': pattern_key,
                    'operation': group['operation'],
                    'ns': group.get('ns', ''),
                    'count': group['count'],
                    'avg_time': avg_time,
                    'max_time': max_time,
                    'example_query': group['example_query']
                })
        
        query_patterns.sort(key=lambda x: x['count'] * x['avg_time'], reverse=True)
        return query_patterns
        
    except Exception as e:
        logger.error("Slow query analysis failed: %s", e, exc_info=True)
        return []

def create_query_pattern_key(command, operation):
    """Create a normalized key for grouping similar queries"""
    try:
        # Get collection name
        collection = ''
        for cmd in ['find', 'aggregate', 'update', 'delete', 'count', 'distinct']:
            if cmd in command:
                collection = command[cmd]
                break

        if 'pipeline' in command and isinstance(command['pipeline'], list):
            # Aggregate: use collection + pipeline stage types + match fields
            stages = [list(s.keys())[0] for s in command['pipeline'] if isinstance(s, dict) and s]
            match_fields = []
            for s in command['pipeline']:
                if isinstance(s, dict) and '$match' in s and isinstance(s['$match'], dict):
                    match_fields = sorted(extract_field_names(s['$match']))
            pattern_key = f"{operation}:{collection}:{','.join(stages)}:{','.join(match_fields)}"
        else:
            # find/update/etc: use filter + sort + projection fields
            filter_obj = command.get('filter') or command.get('q') or {}
            sort_obj = command.get('sort') or {}
            projection_obj = command.get('projection') or command.get('fields') or {}
            filter_fields = extract_field_names(filter_obj) if isinstance(filter_obj, dict) else []
            sort_fields = list(sort_obj.keys()) if isinstance(sort_obj, dict) else []
            projection_fields = list(projection_obj.keys()) if isinstance(projection_obj, dict) else []
            pattern_key = f"{operation}:{collection}:{','.join(sorted(filter_fields))}:{','.join(sorted(sort_fields))}:{','.join(sorted(projection_fields))}"

        return pattern_key

    except Exception:
        return f"{operation}:unknown"

def extract_field_names(obj, prefix=''):
    """Recursively extract field names from query object"""
    fields = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key.startswith('$'):
                continue
            current_field = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict) and not any(k.startswith('$') for k in value.keys()):
                fields.extend(extract_field_names(value, current_field))
            else:
                fields.append(current_field)
    return fields

def extract_collection_from_pattern(pattern):
    """Extract collection name from query pattern"""
    try:
        example_query = pattern.get('example_query', {})
        for cmd in ['find', 'update', 'delete', 'aggregate', 'count']:
            if cmd in example_query:
                return example_query[cmd]
        if 'ns' in example_query:
            ns = example_query['ns']
            if '.' in ns:
                return ns.split('.', 1)[1]
        return 'unknown'
    except:
        return 'unknown'