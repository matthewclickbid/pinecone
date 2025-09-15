"""Pytest configuration and shared fixtures."""

import os
import sys
import pytest
from unittest.mock import Mock, patch
from datetime import datetime
import json

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

@pytest.fixture
def mock_env_vars():
    """Set up mock environment variables."""
    env_vars = {
        'METABASE_URL': 'https://metabase.example.com',
        'METABASE_API_KEY': 'test-metabase-key',
        'METABASE_QUESTION_ID': '123',
        'OPENAI_API_KEY': 'test-openai-key',
        'PINECONE_API_KEY': 'test-pinecone-key',
        'PINECONE_INDEX_NAME': 'test-index',
        'PINECONE_NAMESPACE': 'test-namespace',
        'DYNAMODB_TABLE_NAME': 'test-table',
        'S3_BUCKET_NAME': 'test-bucket',
        'API_KEY': 'test-api-key',
        'ASYNC_LAMBDA_NAME': 'test-async-lambda',
        'STATE_MACHINE_ARN': 'arn:aws:states:us-east-1:123456789012:stateMachine:test'
    }
    
    with patch.dict(os.environ, env_vars):
        yield env_vars

@pytest.fixture
def mock_dynamodb_client():
    """Mock DynamoDB client."""
    with patch('boto3.resource') as mock_resource:
        mock_table = Mock()
        mock_resource.return_value.Table.return_value = mock_table
        yield mock_table

@pytest.fixture
def mock_s3_client():
    """Mock S3 client."""
    with patch('boto3.client') as mock_client:
        mock_s3 = Mock()
        mock_client.return_value = mock_s3
        yield mock_s3

@pytest.fixture
def mock_lambda_client():
    """Mock Lambda client."""
    with patch('boto3.client') as mock_client:
        mock_lambda = Mock()
        mock_client.return_value = mock_lambda
        yield mock_lambda

@pytest.fixture
def mock_stepfunctions_client():
    """Mock Step Functions client."""
    with patch('boto3.client') as mock_client:
        mock_sf = Mock()
        mock_client.return_value = mock_sf
        yield mock_sf

@pytest.fixture
def sample_metabase_data():
    """Sample Metabase response data."""
    return [
        {
            'user_id': 'user1',
            'datetime': '2025-01-01T10:00:00Z',
            'content': 'Test content 1'
        },
        {
            'user_id': 'user2',
            'datetime': '2025-01-01T11:00:00Z',
            'content': 'Test content 2'
        }
    ]

@pytest.fixture
def sample_csv_data():
    """Sample CSV data."""
    return """user_id,datetime,content
user1,2025-01-01T10:00:00Z,Test content 1
user2,2025-01-01T11:00:00Z,Test content 2
"""

@pytest.fixture
def sample_embeddings():
    """Sample OpenAI embeddings."""
    return [
        [0.1] * 1536,  # Standard embedding dimension
        [0.2] * 1536
    ]

@pytest.fixture
def sample_task():
    """Sample DynamoDB task."""
    return {
        'task_id': 'test-task-123',
        'status': 'processing',
        'created_at': '2025-01-01T12:00:00Z',
        'updated_at': '2025-01-01T12:00:00Z',
        'total_records': 0,
        'processed_records': 0,
        'errors': []
    }

@pytest.fixture
def mock_openai_client():
    """Mock OpenAI client."""
    with patch('openai.OpenAI') as mock_openai:
        mock_client = Mock()
        mock_openai.return_value = mock_client
        mock_response = Mock()
        mock_response.data = [Mock(embedding=[0.1] * 1536)]
        mock_client.embeddings.create.return_value = mock_response
        yield mock_client

@pytest.fixture
def mock_pinecone():
    """Mock Pinecone client."""
    with patch('pinecone.Pinecone') as mock_pc:
        mock_index = Mock()
        mock_pc.return_value.Index.return_value = mock_index
        yield mock_index