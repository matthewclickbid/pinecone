"""Integration tests for main Lambda handler."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import json
import os

from main_handler import handler

class TestMainHandler:
    """Test main Lambda handler integration."""
    
    @patch.dict(os.environ, {
        'API_KEY': 'test-api-key',
        'ASYNC_LAMBDA_NAME': 'test-async-lambda',
        'STATE_MACHINE_ARN': 'arn:aws:states:us-east-1:123456789012:stateMachine:test',
        'DYNAMODB_TABLE_NAME': 'test-table'
    })
    @patch('main_handler.boto3.client')
    @patch('main_handler.DynamoDBClient')
    def test_process_endpoint_metabase_success(self, mock_db_class, mock_boto_client):
        """Test successful Metabase processing request."""
        # Setup mocks
        mock_db = Mock()
        mock_db.create_task.return_value = 'test-task-123'
        mock_db_class.return_value = mock_db
        
        mock_lambda = Mock()
        mock_lambda.invoke.return_value = {'StatusCode': 202}
        mock_boto_client.return_value = mock_lambda
        
        # Create event
        event = {
            'httpMethod': 'GET',
            'path': '/process',
            'headers': {'x-api-key': 'test-api-key'},
            'queryStringParameters': {
                'start_date': '2025-01-01',
                'question_id': '123'
            }
        }
        
        response = handler(event, {})
        
        assert response['statusCode'] == 202
        body = json.loads(response['body'])
        assert body['task_id'] == 'test-task-123'
        assert body['status'] == 'processing'
        
        # Verify DynamoDB task was created
        mock_db.create_task.assert_called_once()
        
        # Verify Lambda was invoked
        mock_lambda.invoke.assert_called_once()
        invoke_args = mock_lambda.invoke.call_args[1]
        assert invoke_args['FunctionName'] == 'test-async-lambda'
        assert invoke_args['InvocationType'] == 'Event'
    
    @patch.dict(os.environ, {
        'API_KEY': 'test-api-key',
        'STATE_MACHINE_ARN': 'arn:aws:states:us-east-1:123456789012:stateMachine:test',
        'DYNAMODB_TABLE_NAME': 'test-table'
    })
    @patch('main_handler.boto3.client')
    @patch('main_handler.DynamoDBClient')
    def test_process_endpoint_s3_csv_success(self, mock_db_class, mock_boto_client):
        """Test successful S3 CSV processing request."""
        # Setup mocks
        mock_db = Mock()
        mock_db.create_task.return_value = 'test-task-123'
        mock_db_class.return_value = mock_db
        
        mock_sf = Mock()
        mock_sf.start_execution.return_value = {
            'executionArn': 'arn:aws:states:us-east-1:123456789012:execution:test:exec-123'
        }
        mock_boto_client.return_value = mock_sf
        
        # Create event
        event = {
            'httpMethod': 'GET',
            'path': '/process',
            'headers': {'x-api-key': 'test-api-key'},
            'queryStringParameters': {
                'data_source': 's3_csv',
                's3_key': 'data/test.csv'
            }
        }
        
        response = handler(event, {})
        
        assert response['statusCode'] == 202
        body = json.loads(response['body'])
        assert body['task_id'] == 'test-task-123'
        assert body['status'] == 'processing'
        
        # Verify Step Functions was triggered
        mock_sf.start_execution.assert_called_once()
        exec_args = mock_sf.start_execution.call_args[1]
        assert 'stateMachineArn' in exec_args
        input_data = json.loads(exec_args['input'])
        assert input_data['task_id'] == 'test-task-123'
        assert input_data['s3_key'] == 'data/test.csv'
    
    @patch.dict(os.environ, {'API_KEY': 'test-api-key'})
    def test_process_endpoint_missing_params(self):
        """Test processing request with missing parameters."""
        event = {
            'httpMethod': 'GET',
            'path': '/process',
            'headers': {'x-api-key': 'test-api-key'},
            'queryStringParameters': {}  # Missing required params
        }
        
        response = handler(event, {})
        
        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'error' in body
    
    @patch.dict(os.environ, {'API_KEY': 'test-api-key'})
    def test_process_endpoint_invalid_api_key(self):
        """Test processing request with invalid API key."""
        event = {
            'httpMethod': 'GET',
            'path': '/process',
            'headers': {'x-api-key': 'wrong-key'},
            'queryStringParameters': {
                'start_date': '2025-01-01'
            }
        }
        
        response = handler(event, {})
        
        assert response['statusCode'] == 401
        body = json.loads(response['body'])
        assert body['error'] == 'Unauthorized'
    
    @patch.dict(os.environ, {
        'API_KEY': 'test-api-key',
        'DYNAMODB_TABLE_NAME': 'test-table'
    })
    @patch('main_handler.DynamoDBClient')
    def test_status_endpoint_success(self, mock_db_class):
        """Test successful status check."""
        # Setup mock
        mock_db = Mock()
        mock_db.get_task.return_value = {
            'task_id': 'test-task-123',
            'status': 'completed',
            'total_records': 100,
            'processed_records': 100,
            'created_at': '2025-01-01T10:00:00Z',
            'updated_at': '2025-01-01T10:05:00Z'
        }
        mock_db_class.return_value = mock_db
        
        event = {
            'httpMethod': 'GET',
            'path': '/status',
            'headers': {'x-api-key': 'test-api-key'},
            'queryStringParameters': {
                'task_id': 'test-task-123'
            }
        }
        
        response = handler(event, {})
        
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['task_id'] == 'test-task-123'
        assert body['status'] == 'completed'
        assert body['total_records'] == 100
        assert body['processed_records'] == 100
    
    @patch.dict(os.environ, {
        'API_KEY': 'test-api-key',
        'DYNAMODB_TABLE_NAME': 'test-table'
    })
    @patch('main_handler.DynamoDBClient')
    def test_status_endpoint_task_not_found(self, mock_db_class):
        """Test status check for non-existent task."""
        # Setup mock
        mock_db = Mock()
        mock_db.get_task.return_value = None
        mock_db_class.return_value = mock_db
        
        event = {
            'httpMethod': 'GET',
            'path': '/status',
            'headers': {'x-api-key': 'test-api-key'},
            'queryStringParameters': {
                'task_id': 'non-existent'
            }
        }
        
        response = handler(event, {})
        
        assert response['statusCode'] == 404
        body = json.loads(response['body'])
        assert body['error'] == 'Task not found'
    
    @patch.dict(os.environ, {'API_KEY': 'test-api-key'})
    def test_status_endpoint_missing_task_id(self):
        """Test status check without task_id."""
        event = {
            'httpMethod': 'GET',
            'path': '/status',
            'headers': {'x-api-key': 'test-api-key'},
            'queryStringParameters': {}
        }
        
        response = handler(event, {})
        
        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'error' in body
        assert 'task_id' in body['error'].lower()
    
    def test_invalid_http_method(self):
        """Test handling of invalid HTTP method."""
        event = {
            'httpMethod': 'POST',  # Should be GET
            'path': '/process',
            'headers': {'x-api-key': 'test-api-key'}
        }
        
        response = handler(event, {})
        
        assert response['statusCode'] == 405
        body = json.loads(response['body'])
        assert body['error'] == 'Method not allowed'
    
    def test_invalid_path(self):
        """Test handling of invalid path."""
        event = {
            'httpMethod': 'GET',
            'path': '/invalid',
            'headers': {'x-api-key': 'test-api-key'}
        }
        
        response = handler(event, {})
        
        assert response['statusCode'] == 404
        body = json.loads(response['body'])
        assert body['error'] == 'Not found'