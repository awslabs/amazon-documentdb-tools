"""
Pytest configuration and shared fixtures for DocumentDB Profiler tests
"""
import os
import pytest
from moto import mock_aws
import boto3


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def logs_client(aws_credentials):
    """Create a mocked CloudWatch Logs client."""
    with mock_aws():
        yield boto3.client("logs", region_name="us-east-1")


@pytest.fixture(scope="function")
def ses_client(aws_credentials):
    """Create a mocked SES client."""
    with mock_aws():
        yield boto3.client("ses", region_name="us-east-1")


@pytest.fixture
def clean_env():
    """Clean environment variables before and after test."""
    # Store original env vars
    original_env = dict(os.environ)
    
    # Clear relevant env vars
    env_vars_to_clear = [
        'DOCDB_LOG_GROUP_NAME',
        'SENDER_EMAIL', 
        'RECIPIENT_EMAIL_LIST',
        'TOP_OPS_COUNT',
        'REPORT_START_TIME',
        'REPORT_END_TIME'
    ]
    
    for var in env_vars_to_clear:
        if var in os.environ:
            del os.environ[var]
    
    yield
    
    # Restore original env vars
    os.environ.clear()
    os.environ.update(original_env)