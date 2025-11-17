import json
import os
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime

# Import the actual modules
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))
from lambda_handler import lambda_handler
from docdb_profiler_analyzer import DocDBProfilerAnalyzer, analyze_profiler_logs


class MockLambdaContext:
    """Mock Lambda context for testing"""
    def __init__(self):
        self.function_name = "DocDBProfilerFunction"
        self.memory_limit_in_mb = 128
        self.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:DocDBProfilerFunction"
        self.aws_request_id = "52fdfc07-2182-154f-163f-5f0f9a621d72"

    def get_remaining_time_in_millis(self) -> int:
        return 30000


@pytest.fixture
def lambda_context():
    """Fixture for Lambda context"""
    return MockLambdaContext()


@pytest.fixture
def scheduler_event():
    """EventBridge Scheduler event fixture"""
    return {
        "version": "0",
        "id": "53dc4d37-cffa-4f76-80c9-8b7d4a4d2eaa",
        "detail-type": "Scheduled Event",
        "source": "aws.scheduler",
        "account": "123456789012",
        "time": "2024-02-11T13:00:00Z",
        "region": "us-east-1",
        "resources": [
            "arn:aws:scheduler:us-east-1:123456789012:schedule/default/docdb-profiler-daily-schedule"
        ],
        "detail": {}
    }


@pytest.fixture
def manual_event():
    """Manual invocation event fixture"""
    return {
        "test": True,
        "description": "Manual test invocation for DocumentDB profiler"
    }


@pytest.fixture
def mock_env_vars():
    """Mock environment variables"""
    return {
        'DOCDB_LOG_GROUP_NAME': '/aws/docdb/test-cluster/profiler',
        'SENDER_EMAIL': 'sender@example.com',
        'RECIPIENT_EMAIL_LIST': 'recipient@example.com',
        'TOP_OPS_COUNT': '10',
        'POWERTOOLS_SERVICE_NAME': 'DocDBProfilerService',
        'POWERTOOLS_METRICS_NAMESPACE': 'DocDBProfilerMetrics'
    }


@pytest.fixture
def sample_log_data():
    """Sample DocumentDB profiler log data"""
    return [
        {
            'timestamp': '2024-02-11T13:00:00.000Z',
            'op': 'query',
            'ns': 'testdb.users',
            'user': 'testuser',
            'client': '10.0.0.1:12345',
            'command': {'find': 'users', 'filter': {'status': 'active'}},
            'execStats': {'nReturned': 100, 'executionTimeMillisEstimate': 50},
            'millis': 45,
            'nreturned': 100
        },
        {
            'timestamp': '2024-02-11T13:01:00.000Z',
            'op': 'command',
            'ns': 'testdb.orders',
            'user': 'testuser',
            'client': '10.0.0.2:12346',
            'command': {'aggregate': 'orders', 'pipeline': [{'$match': {'date': '2024-02-11'}}]},
            'execStats': {'nReturned': 50, 'executionTimeMillisEstimate': 120},
            'millis': 115,
            'nreturned': 50
        }
    ]


def test_parse_log_groups():
    """Test log group parsing function"""
    log_groups_str = "/aws/docdb/cluster1/profiler,/aws/docdb/cluster2/profiler"
    result = DocDBProfilerAnalyzer.parse_log_groups(log_groups_str)
    
    assert len(result) == 2
    assert result[0]['cluster_name'] == 'cluster1'
    assert result[0]['profiler_log'] == '/aws/docdb/cluster1/profiler'
    assert result[1]['cluster_name'] == 'cluster2'
    assert result[1]['profiler_log'] == '/aws/docdb/cluster2/profiler'


@patch('docdb_profiler_analyzer.boto3.client')
@patch('docdb_profiler_analyzer.boto3.Session')
def test_docdb_profiler_analyzer_init(mock_session, mock_client):
    """Test DocDBProfilerAnalyzer initialization"""
    mock_session.return_value.region_name = 'us-east-1'
    
    log_group = {
        'cluster_name': 'test-cluster',
        'profiler_log': '/aws/docdb/test-cluster/profiler'
    }
    
    analyzer = DocDBProfilerAnalyzer(
        log_group=log_group,
        top_ops_count=10,
        sender_email='sender@example.com',
        recipient_emails='recipient@example.com'
    )
    
    assert analyzer.cluster_name == 'test-cluster'
    assert analyzer.log_group_name == '/aws/docdb/test-cluster/profiler'
    assert analyzer.top_ops_count == 10
    assert analyzer.sender_email_address == 'sender@example.com'
    assert analyzer.recipient_email_list == ['recipient@example.com']


def test_lambda_handler_missing_docdb_log_group(scheduler_event, lambda_context):
    """Test lambda handler with missing DOCDB_LOG_GROUP_NAME environment variable"""
    with patch.dict(os.environ, {}, clear=True):
        result = lambda_handler(scheduler_event, lambda_context)
        
        assert result['statusCode'] == 500
        assert 'DOCDB_LOG_GROUP_NAME environment variable is not set' in result['body']


def test_lambda_handler_missing_sender_email(scheduler_event, lambda_context):
    """Test lambda handler with missing SENDER_EMAIL environment variable"""
    with patch.dict(os.environ, {'DOCDB_LOG_GROUP_NAME': '/aws/docdb/test-cluster/profiler'}, clear=True):
        result = lambda_handler(scheduler_event, lambda_context)
        
        assert result['statusCode'] == 500
        assert 'SENDER_EMAIL environment variable is not set' in result['body']


def test_lambda_handler_missing_recipient_email(scheduler_event, lambda_context):
    """Test lambda handler with missing RECIPIENT_EMAIL_LIST environment variable"""
    with patch.dict(os.environ, {
        'DOCDB_LOG_GROUP_NAME': '/aws/docdb/test-cluster/profiler',
        'SENDER_EMAIL': 'sender@example.com'
    }, clear=True):
        result = lambda_handler(scheduler_event, lambda_context)
        
        assert result['statusCode'] == 500
        assert 'RECIPIENT_EMAIL_LIST environment variable is not set' in result['body']


@patch('lambda_handler.analyze_profiler_logs')
def test_lambda_handler_no_logs_found(mock_analyze, scheduler_event, lambda_context):
    """Test lambda handler when no logs are found"""
    # Mock analyze_profiler_logs to return empty results
    mock_analyze.return_value = []
    
    with patch.dict(os.environ, {
        'DOCDB_LOG_GROUP_NAME': '/aws/docdb/test-cluster/profiler',
        'SENDER_EMAIL': 'sender@example.com',
        'RECIPIENT_EMAIL_LIST': 'recipient@example.com'
    }):
        result = lambda_handler(scheduler_event, lambda_context)
        
        assert result['statusCode'] == 200
        assert result['body'] == "No profiler logs found"


@patch('lambda_handler.analyze_profiler_logs')
def test_lambda_handler_with_manual_event(mock_analyze, manual_event, lambda_context):
    """Test lambda handler with manual invocation event"""
    # Mock analyze_profiler_logs to return empty results
    mock_analyze.return_value = []
    
    with patch.dict(os.environ, {
        'DOCDB_LOG_GROUP_NAME': '/aws/docdb/test-cluster/profiler',
        'SENDER_EMAIL': 'sender@example.com',
        'RECIPIENT_EMAIL_LIST': 'recipient@example.com'
    }):
        result = lambda_handler(manual_event, lambda_context)
        
        assert result['statusCode'] == 200


@patch('docdb_profiler_analyzer.CloudWatchQuery')
@patch('docdb_profiler_analyzer.boto3.client')
@patch('docdb_profiler_analyzer.boto3.Session')
def test_analyze_profiler_logs_success(mock_session, mock_client, mock_cloudwatch_query, sample_log_data):
    """Test analyze_profiler_logs function with successful log processing using CloudWatchQuery"""
    mock_session.return_value.region_name = 'us-east-1'
    
    # Mock CloudWatchQuery instance
    mock_cw_query_instance = Mock()
    
    # Create mock log entries that match the expected format from CloudWatchQuery
    mock_log_entries = []
    for log_entry in sample_log_data:
        mock_log_entries.append([
            {"field": "@timestamp", "value": log_entry['timestamp']},
            {"field": "@message", "value": json.dumps(log_entry)}
        ])
    
    mock_cw_query_instance.query_results = mock_log_entries
    mock_cw_query_instance.query_duration = 2.5
    mock_cloudwatch_query.return_value = mock_cw_query_instance
    
    # Test the function
    results = analyze_profiler_logs(
        log_groups_str="/aws/docdb/test-cluster/profiler",
        top_ops_count=10,
        sender_email="sender@example.com",
        recipient_emails="recipient@example.com"
    )
    
    assert len(results) == 1
    assert results[0]['cluster_name'] == 'test-cluster'
    assert results[0]['log_group'] == '/aws/docdb/test-cluster/profiler'
    assert 'top_operations' in results[0]
    assert 'time_range' in results[0]
    assert 'analyzer' in results[0]
    
    # Verify CloudWatchQuery was instantiated correctly
    mock_cloudwatch_query.assert_called_once_with(
        log_group='/aws/docdb/test-cluster/profiler',
        query_string='\n            fields @timestamp, @message\n            | sort @timestamp asc\n            '
    )
    
    # Verify query_logs was called
    mock_cw_query_instance.query_logs.assert_called_once()


@patch('docdb_profiler_analyzer.CloudWatchQuery')
@patch('docdb_profiler_analyzer.boto3.client')
@patch('docdb_profiler_analyzer.boto3.Session')
def test_analyze_profiler_logs_multiple_clusters(mock_session, mock_client, mock_cloudwatch_query):
    """Test analyze_profiler_logs function with multiple clusters using CloudWatchQuery"""
    mock_session.return_value.region_name = 'us-east-1'
    
    # Mock CloudWatchQuery instance with empty results
    mock_cw_query_instance = Mock()
    mock_cw_query_instance.query_results = []  # Empty results
    mock_cw_query_instance.query_duration = 1.0
    mock_cloudwatch_query.return_value = mock_cw_query_instance
    
    # Test with multiple log groups
    results = analyze_profiler_logs(
        log_groups_str="/aws/docdb/cluster1/profiler,/aws/docdb/cluster2/profiler",
        top_ops_count=5
    )
    
    # Should return empty list since no logs found
    assert len(results) == 0
    
    # Verify CloudWatchQuery was called twice (once for each cluster)
    assert mock_cloudwatch_query.call_count == 2


def test_analyze_profiler_logs_empty_log_groups():
    """Test analyze_profiler_logs function with empty log groups"""
    # Empty string should return empty results, not raise exception
    results = analyze_profiler_logs(log_groups_str="")
    assert results == []


@patch('lambda_handler.analyze_profiler_logs')
def test_lambda_handler_successful_email_send(mock_analyze, scheduler_event, lambda_context):
    """Test lambda handler with successful log analysis and email sending"""
    # Mock successful analysis results
    mock_analyzer = Mock()
    mock_analyzer.send_response_via_email.return_value = {'MessageId': 'test-message-id'}
    
    mock_top_ops = pd.DataFrame([
        {
            'operation': 'query',
            'namespace': 'testdb.users',
            'user': 'testuser',
            'appName': 'testapp',
            'filter_criterion': "{'status': 'active'}",
            'execution_stats': "{'nReturned': 100}",
            'avg_duration_ms': 45,
            'max_duration_ms': 50,
            'count': 1
        }
    ])
    
    mock_results = [{
        'cluster_name': 'test-cluster',
        'log_group': '/aws/docdb/test-cluster/profiler',
        'top_operations': mock_top_ops,
        'time_range': {
            'start': datetime.now(),
            'end': datetime.now()
        },
        'analyzer': mock_analyzer
    }]
    
    mock_analyze.return_value = mock_results
    
    with patch.dict(os.environ, {
        'DOCDB_LOG_GROUP_NAME': '/aws/docdb/test-cluster/profiler',
        'SENDER_EMAIL': 'sender@example.com',
        'RECIPIENT_EMAIL_LIST': 'recipient@example.com'
    }):
        result = lambda_handler(scheduler_event, lambda_context)
        
        assert result['statusCode'] == 200
        assert result['body'] == "Top operations report published."
        mock_analyzer.send_response_via_email.assert_called_once()


@patch('lambda_handler.analyze_profiler_logs')
def test_lambda_handler_with_time_range_env_vars(mock_analyze, scheduler_event, lambda_context):
    """Test lambda handler with custom time range from environment variables"""
    mock_analyze.return_value = []
    
    with patch.dict(os.environ, {
        'DOCDB_LOG_GROUP_NAME': '/aws/docdb/test-cluster/profiler',
        'SENDER_EMAIL': 'sender@example.com',
        'RECIPIENT_EMAIL_LIST': 'recipient@example.com',
        'REPORT_START_TIME': '2024-01-01 00:00:00',
        'REPORT_END_TIME': '2024-01-02 00:00:00',
        'TOP_OPS_COUNT': '20'
    }):
        result = lambda_handler(scheduler_event, lambda_context)
        
        # Verify analyze_profiler_logs was called with correct parameters
        mock_analyze.assert_called_once()
        call_args = mock_analyze.call_args
        
        assert call_args.kwargs['log_groups_str'] == '/aws/docdb/test-cluster/profiler'
        assert call_args.kwargs['top_ops_count'] == 20
        assert call_args.kwargs['sender_email'] == 'sender@example.com'
        assert call_args.kwargs['recipient_emails'] == 'recipient@example.com'
        assert call_args.kwargs['start_time'] is not None
        assert call_args.kwargs['end_time'] is not None


def test_docdb_profiler_analyzer_parse_log_groups_edge_cases():
    """Test edge cases for log group parsing"""
    # Test with whitespace
    result = DocDBProfilerAnalyzer.parse_log_groups(" /aws/docdb/cluster1/profiler , /aws/docdb/cluster2/profiler ")
    assert len(result) == 2
    assert result[0]['cluster_name'] == 'cluster1'
    
    # Test with single log group
    result = DocDBProfilerAnalyzer.parse_log_groups("/aws/docdb/single-cluster/profiler")
    assert len(result) == 1
    assert result[0]['cluster_name'] == 'single-cluster'
    
    # Test with invalid format (should return empty list since it doesn't have enough parts)
    result = DocDBProfilerAnalyzer.parse_log_groups("/invalid/format")
    assert len(result) == 0  # Should return empty list for invalid format


@patch('docdb_profiler_analyzer.boto3.client')
@patch('docdb_profiler_analyzer.boto3.Session')
def test_docdb_profiler_analyzer_with_none_email_params(mock_session, mock_client):
    """Test DocDBProfilerAnalyzer initialization with None email parameters (CLI mode)"""
    mock_session.return_value.region_name = 'us-east-1'
    
    log_group = {
        'cluster_name': 'test-cluster',
        'profiler_log': '/aws/docdb/test-cluster/profiler'
    }
    
    analyzer = DocDBProfilerAnalyzer(
        log_group=log_group,
        top_ops_count=None,
        sender_email=None,
        recipient_emails=None
    )
    
    assert analyzer.cluster_name == 'test-cluster'
    assert analyzer.log_group_name == '/aws/docdb/test-cluster/profiler'
    assert analyzer.top_ops_count is None
    assert analyzer.sender_email_address is None
    assert analyzer.recipient_email_list == []


def test_lambda_handler_exception_handling(scheduler_event, lambda_context):
    """Test lambda handler exception handling"""
    with patch('lambda_handler.analyze_profiler_logs') as mock_analyze:
        mock_analyze.side_effect = Exception("Test error")
        
        with patch.dict(os.environ, {
            'DOCDB_LOG_GROUP_NAME': '/aws/docdb/test-cluster/profiler',
            'SENDER_EMAIL': 'sender@example.com',
            'RECIPIENT_EMAIL_LIST': 'recipient@example.com'
        }):
            result = lambda_handler(scheduler_event, lambda_context)
            
            assert result['statusCode'] == 500
            assert 'Test error' in result['body']


# Additional test cases for comprehensive coverage

@patch('docdb_profiler_analyzer.CloudWatchQuery')
@patch('docdb_profiler_analyzer.boto3.client')
@patch('docdb_profiler_analyzer.boto3.Session')
def test_docdb_profiler_analyzer_fetch_profiler_logs_with_cloudwatch_query(mock_session, mock_client, mock_cloudwatch_query):
    """Test fetch_profiler_logs using CloudWatchQuery integration"""
    mock_session.return_value.region_name = 'us-east-1'
    
    log_group = {
        'cluster_name': 'test-cluster',
        'profiler_log': '/aws/docdb/test-cluster/profiler'
    }
    
    # Mock CloudWatchQuery instance
    mock_cw_query_instance = Mock()
    mock_cw_query_instance.query_results = [
        [
            {"field": "@timestamp", "value": "2024-02-11T13:00:00.000Z"},
            {"field": "@message", "value": '{"op": "query", "ns": "test.collection", "millis": 100}'}
        ]
    ]
    mock_cw_query_instance.query_duration = 2.5
    mock_cloudwatch_query.return_value = mock_cw_query_instance
    
    analyzer = DocDBProfilerAnalyzer(log_group, None, None, None)
    
    df = analyzer.fetch_profiler_logs(
        start_time=datetime.now(),
        end_time=datetime.now()
    )
    
    # Verify CloudWatchQuery was used correctly
    mock_cloudwatch_query.assert_called_once_with(
        log_group='/aws/docdb/test-cluster/profiler',
        query_string='\n            fields @timestamp, @message\n            | sort @timestamp asc\n            '
    )
    mock_cw_query_instance.query_logs.assert_called_once()
    
    # Verify DataFrame was created
    assert len(df) == 1
    assert df.iloc[0]['op'] == 'query'


@patch('docdb_profiler_analyzer.CloudWatchQuery')
@patch('docdb_profiler_analyzer.boto3.client')
@patch('docdb_profiler_analyzer.boto3.Session')
def test_docdb_profiler_analyzer_malformed_json_logs(mock_session, mock_client, mock_cloudwatch_query):
    """Test handling of malformed JSON in log entries with CloudWatchQuery"""
    mock_session.return_value.region_name = 'us-east-1'
    
    log_group = {
        'cluster_name': 'test-cluster',
        'profiler_log': '/aws/docdb/test-cluster/profiler'
    }
    
    # Mock CloudWatchQuery with malformed JSON results
    mock_cw_query_instance = Mock()
    mock_cw_query_instance.query_results = [
        [
            {"field": "@timestamp", "value": "2024-02-11T13:00:00.000Z"},
            {"field": "@message", "value": "invalid json {"}  # Malformed JSON
        ],
        [
            {"field": "@timestamp", "value": "2024-02-11T13:01:00.000Z"},
            {"field": "@message", "value": '{"op": "query", "ns": "test.collection", "millis": 100}'}  # Valid JSON
        ]
    ]
    mock_cw_query_instance.query_duration = 1.5
    mock_cloudwatch_query.return_value = mock_cw_query_instance
    
    analyzer = DocDBProfilerAnalyzer(log_group, None, None, None)
    
    df = analyzer.fetch_profiler_logs(
        start_time=datetime.now(),
        end_time=datetime.now()
    )
    
    # Should only have 1 valid entry
    assert len(df) == 1
    assert df.iloc[0]['op'] == 'query'


def test_docdb_profiler_analyzer_process_filter():
    """Test _process_filter method with various operation types"""
    log_group = {'cluster_name': 'test', 'profiler_log': '/test'}
    
    with patch('docdb_profiler_analyzer.boto3.Session') as mock_session:
        mock_session.return_value.region_name = 'us-east-1'
        
        analyzer = DocDBProfilerAnalyzer(log_group, None, None, None)
        
        # Test data with different operation types
        test_data = pd.DataFrame([
            {
                'op': 'query',
                'command': {'find': 'users', 'filter': {'status': 'active', 'age': 25}}
            },
            {
                'op': 'command',
                'command': {'aggregate': 'orders', 'pipeline': [{'$match': {'date': '2024-01-01'}}]}
            }
        ])
        
        result = analyzer._process_filter(test_data)
        
        assert 'modified_filter' in result.columns
        assert len(result) == 2
        
        # Check that values are replaced with placeholders
        query_filter = result.iloc[0]['modified_filter']
        assert 'V1' in str(query_filter) or 'V2' in str(query_filter)


def test_docdb_profiler_analyzer_process_exec_stats():
    """Test _process_exec_stats method"""
    log_group = {'cluster_name': 'test', 'profiler_log': '/test'}
    
    with patch('docdb_profiler_analyzer.boto3.Session') as mock_session:
        mock_session.return_value.region_name = 'us-east-1'
        
        analyzer = DocDBProfilerAnalyzer(log_group, None, None, None)
        
        # Test data with execStats
        test_data = pd.DataFrame([
            {
                'op': 'query',
                'execStats': {'nReturned': 100, 'executionTimeMillisEstimate': 50, 'stage': 'COLLSCAN'}
            },
            {
                'op': 'command',
                'execStats': {'nReturned': 25, 'executionTimeMillisEstimate': 120}
            }
        ])
        
        result = analyzer._process_exec_stats(test_data)
        
        assert 'modified_execStats' in result.columns
        assert len(result) == 2
        
        # Check that sensitive values are replaced with '?'
        exec_stats = result.iloc[0]['modified_execStats']
        assert exec_stats['nReturned'] == '?'
        assert exec_stats['executionTimeMillisEstimate'] == '?'
        assert exec_stats['stage'] == 'COLLSCAN'  # Other fields should remain


def test_docdb_profiler_analyzer_get_top_operations():
    """Test get_top_operations method with complete data processing"""
    log_group = {'cluster_name': 'test', 'profiler_log': '/test'}
    
    with patch('docdb_profiler_analyzer.boto3.Session') as mock_session:
        mock_session.return_value.region_name = 'us-east-1'
        
        analyzer = DocDBProfilerAnalyzer(log_group, top_ops_count=5, sender_email=None, recipient_emails=None)
        
        # Test data with all required fields
        test_data = pd.DataFrame([
            {
                'op': 'query',
                'ns': 'testdb.users',
                'user': 'testuser',
                'appName': 'testapp',
                'command': {'find': 'users', 'filter': {'status': 'active'}},
                'execStats': {'nReturned': 100, 'executionTimeMillisEstimate': 50},
                'millis': 45,
                'nreturned': 100
            },
            {
                'op': 'query',
                'ns': 'testdb.users',
                'user': 'testuser',
                'appName': 'testapp',
                'command': {'find': 'users', 'filter': {'status': 'active'}},
                'execStats': {'nReturned': 150, 'executionTimeMillisEstimate': 75},
                'millis': 70,
                'nreturned': 150
            }
        ])
        
        result = analyzer.get_top_operations(test_data)
        
        assert len(result) == 1  # Should be grouped into one operation
        assert 'operation' in result.columns
        assert 'namespace' in result.columns
        assert 'avg_duration_ms' in result.columns
        assert 'max_duration_ms' in result.columns
        assert 'count' in result.columns
        
        # Check aggregated values
        assert result.iloc[0]['count'] == 2
        assert result.iloc[0]['avg_duration_ms'] == 57  # (45 + 70) / 2
        assert result.iloc[0]['max_duration_ms'] == 70


def test_docdb_profiler_analyzer_get_top_operations_missing_appname():
    """Test get_top_operations with missing appName column"""
    log_group = {'cluster_name': 'test', 'profiler_log': '/test'}
    
    with patch('docdb_profiler_analyzer.boto3.Session') as mock_session:
        mock_session.return_value.region_name = 'us-east-1'
        
        analyzer = DocDBProfilerAnalyzer(log_group, None, None, None)
        
        # Test data without appName
        test_data = pd.DataFrame([
            {
                'op': 'query',
                'ns': 'testdb.users',
                'user': 'testuser',
                'command': {'find': 'users'},
                'execStats': {},
                'millis': 45
            }
        ])
        
        result = analyzer.get_top_operations(test_data)
        
        assert len(result) == 1
        assert result.iloc[0]['appName'] == 'N/A'


@patch('docdb_profiler_analyzer.boto3.client')
def test_docdb_profiler_analyzer_send_response_via_email(mock_client):
    """Test send_response_via_email method"""
    log_group = {'cluster_name': 'test-cluster', 'profiler_log': '/test'}
    
    with patch('docdb_profiler_analyzer.boto3.Session') as mock_session:
        mock_session.return_value.region_name = 'us-east-1'
        
        # Mock SES client
        mock_ses_client = Mock()
        mock_ses_client.send_email.return_value = {'MessageId': 'test-message-id'}
        mock_client.return_value = mock_ses_client
        
        analyzer = DocDBProfilerAnalyzer(
            log_group, 
            top_ops_count=10,
            sender_email='sender@example.com',
            recipient_emails='recipient1@example.com,recipient2@example.com'
        )
        
        response_data = {
            'top_operations': [
                {
                    'operation': 'query',
                    'namespace': 'testdb.users',
                    'user': 'testuser',
                    'appName': 'testapp',
                    'filter_criterion': "{'status': 'active'}",
                    'execution_stats': "{'nReturned': '?'}",
                    'avg_duration_ms': 45,
                    'max_duration_ms': 50,
                    'count': 1
                }
            ],
            'time_range': {
                'start': '2024-02-11T00:00:00',
                'end': '2024-02-11T23:59:59'
            }
        }
        
        result = analyzer.send_response_via_email(response_data)
        
        assert result['MessageId'] == 'test-message-id'
        mock_ses_client.send_email.assert_called_once()
        
        # Verify email parameters
        call_args = mock_ses_client.send_email.call_args
        assert call_args[1]['Source'] == 'sender@example.com'
        assert call_args[1]['Destination']['ToAddresses'] == ['recipient1@example.com', 'recipient2@example.com']


def test_docdb_profiler_analyzer_send_email_missing_config():
    """Test send_response_via_email with missing email configuration"""
    log_group = {'cluster_name': 'test', 'profiler_log': '/test'}
    
    with patch('docdb_profiler_analyzer.boto3.Session') as mock_session:
        mock_session.return_value.region_name = 'us-east-1'
        
        analyzer = DocDBProfilerAnalyzer(log_group, None, None, None)
        
        with pytest.raises(ValueError, match="Email configuration missing"):
            analyzer.send_response_via_email({})


def test_docdb_profiler_analyzer_format_response_to_html():
    """Test _format_response_to_html method"""
    log_group = {'cluster_name': 'test-cluster', 'profiler_log': '/test'}
    
    with patch('docdb_profiler_analyzer.boto3.Session') as mock_session:
        mock_session.return_value.region_name = 'us-east-1'
        
        analyzer = DocDBProfilerAnalyzer(log_group, top_ops_count=5, sender_email=None, recipient_emails=None)
        
        test_data = {
            'top_operations': [
                {
                    'operation': 'query',
                    'namespace': 'testdb.users',
                    'user': 'testuser',
                    'appName': 'testapp',
                    'filter_criterion': "{'status': 'active'}",
                    'execution_stats': "{'nReturned': '?'}",
                    'avg_duration_ms': 45,
                    'max_duration_ms': 50,
                    'count': 1
                }
            ],
            'time_range': {
                'start': '2024-02-11T00:00:00',
                'end': '2024-02-11T23:59:59'
            }
        }
        
        html_result = analyzer._format_response_to_html(test_data)
        
        assert '<html>' in html_result
        assert '<table' in html_result
        assert 'test-cluster' in html_result
        assert 'query' in html_result
        assert 'testdb.users' in html_result


@patch('argparse.ArgumentParser.parse_args')
@patch('docdb_profiler_analyzer.analyze_profiler_logs')
@patch('os.makedirs')
def test_main_function_basic(mock_makedirs, mock_analyze, mock_parse_args):
    """Test main function with basic arguments"""
    # Mock command line arguments
    mock_args = Mock()
    mock_args.log_groups = '/aws/docdb/test-cluster/profiler'
    mock_args.top_ops_count = 10
    mock_args.output_dir = './reports'
    mock_args.start_time = None
    mock_args.end_time = None
    mock_parse_args.return_value = mock_args
    
    # Mock analysis results
    mock_top_ops = pd.DataFrame([
        {
            'operation': 'query',
            'namespace': 'testdb.users',
            'avg_duration_ms': 45,
            'max_duration_ms': 50,
            'count': 1
        }
    ])
    
    mock_results = [{
        'cluster_name': 'test-cluster',
        'top_operations': mock_top_ops,
        'time_range': {
            'start': datetime(2024, 2, 11, 0, 0, 0),
            'end': datetime(2024, 2, 11, 23, 59, 59)
        }
    }]
    
    mock_analyze.return_value = mock_results
    
    # Mock pandas to_csv method
    with patch.object(pd.DataFrame, 'to_csv') as mock_to_csv:
        from docdb_profiler_analyzer import main
        
        # Should not raise any exceptions
        main()
        
        # Verify analyze_profiler_logs was called
        mock_analyze.assert_called_once()
        
        # Verify CSV was created
        mock_to_csv.assert_called_once()


@patch('argparse.ArgumentParser.parse_args')
@patch('docdb_profiler_analyzer.analyze_profiler_logs')
def test_main_function_with_time_range(mock_analyze, mock_parse_args):
    """Test main function with custom time range"""
    # Mock command line arguments with time range
    mock_args = Mock()
    mock_args.log_groups = '/aws/docdb/test-cluster/profiler'
    mock_args.top_ops_count = None
    mock_args.output_dir = '.'
    mock_args.start_time = '2024-01-01 00:00:00'
    mock_args.end_time = '2024-01-02 00:00:00'
    mock_parse_args.return_value = mock_args
    
    mock_analyze.return_value = []  # No results
    
    from docdb_profiler_analyzer import main
    
    with patch('os.makedirs'):
        main()
    
    # Verify analyze_profiler_logs was called with parsed datetime objects
    mock_analyze.assert_called_once()
    call_args = mock_analyze.call_args
    
    assert call_args.kwargs['start_time'] == datetime(2024, 1, 1, 0, 0, 0)
    assert call_args.kwargs['end_time'] == datetime(2024, 1, 2, 0, 0, 0)


@patch('argparse.ArgumentParser.parse_args')
@patch('docdb_profiler_analyzer.analyze_profiler_logs')
def test_main_function_no_results(mock_analyze, mock_parse_args):
    """Test main function when no results are found"""
    mock_args = Mock()
    mock_args.log_groups = '/aws/docdb/test-cluster/profiler'
    mock_args.top_ops_count = None
    mock_args.output_dir = '.'
    mock_args.start_time = None
    mock_args.end_time = None
    mock_parse_args.return_value = mock_args
    
    mock_analyze.return_value = []  # No results
    
    from docdb_profiler_analyzer import main
    
    with patch('os.makedirs'):
        # Should not raise any exceptions
        main()
    
    mock_analyze.assert_called_once()


@patch('docdb_profiler_analyzer.CloudWatchQuery')
def test_analyze_profiler_logs_with_custom_time_range(mock_cloudwatch_query):
    """Test analyze_profiler_logs with custom time range using CloudWatchQuery"""
    start_time = datetime(2024, 1, 1, 0, 0, 0)
    end_time = datetime(2024, 1, 2, 0, 0, 0)
    
    # Mock CloudWatchQuery instance
    mock_cw_query_instance = Mock()
    mock_cw_query_instance.query_results = []  # Empty results
    mock_cw_query_instance.query_duration = 1.0
    mock_cloudwatch_query.return_value = mock_cw_query_instance
    
    with patch('docdb_profiler_analyzer.DocDBProfilerAnalyzer') as mock_analyzer_class:
        mock_analyzer = Mock()
        mock_analyzer.fetch_profiler_logs.return_value = pd.DataFrame()  # Empty DataFrame
        mock_analyzer_class.return_value = mock_analyzer
        
        with patch('docdb_profiler_analyzer.DocDBProfilerAnalyzer.parse_log_groups') as mock_parse:
            mock_parse.return_value = [{'cluster_name': 'test', 'profiler_log': '/test'}]
            
            results = analyze_profiler_logs(
                log_groups_str="/aws/docdb/test-cluster/profiler",
                start_time=start_time,
                end_time=end_time
            )
            
            # Should return empty list due to empty DataFrame
            assert results == []
            
            # Verify fetch_profiler_logs was called with correct time range
            mock_analyzer.fetch_profiler_logs.assert_called_once_with(start_time, end_time)


def test_lambda_handler_with_invalid_top_ops_count(scheduler_event, lambda_context):
    """Test lambda handler with invalid TOP_OPS_COUNT environment variable"""
    with patch.dict(os.environ, {
        'DOCDB_LOG_GROUP_NAME': '/aws/docdb/test-cluster/profiler',
        'SENDER_EMAIL': 'sender@example.com',
        'RECIPIENT_EMAIL_LIST': 'recipient@example.com',
        'TOP_OPS_COUNT': 'invalid_number'
    }):
        with patch('lambda_handler.analyze_profiler_logs') as mock_analyze:
            mock_analyze.return_value = []
            
            # Should handle invalid TOP_OPS_COUNT gracefully
            result = lambda_handler(scheduler_event, lambda_context)
            
            # Should still succeed with None top_ops_count
            assert result['statusCode'] == 200
            
            # Verify analyze_profiler_logs was called with None for top_ops_count
            call_args = mock_analyze.call_args
            assert call_args.kwargs['top_ops_count'] is None