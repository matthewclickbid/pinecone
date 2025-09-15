"""Unit tests for DynamoDB client."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import uuid

from services.dynamodb_client import DynamoDBClient

class TestDynamoDBClient:
    """Test DynamoDB client operations."""
    
    @patch('boto3.resource')
    def test_init(self, mock_resource):
        """Test DynamoDBClient initialization."""
        mock_table = Mock()
        mock_resource.return_value.Table.return_value = mock_table
        
        client = DynamoDBClient('test-table')
        
        assert client.table == mock_table
        mock_resource.assert_called_once_with('dynamodb', region_name='us-east-1')
        mock_resource.return_value.Table.assert_called_once_with('test-table')
    
    @patch('boto3.resource')
    def test_create_task(self, mock_resource):
        """Test task creation."""
        mock_table = Mock()
        mock_resource.return_value.Table.return_value = mock_table
        
        client = DynamoDBClient('test-table')
        
        params = {
            'start_date': '2025-01-01',
            'question_id': '123',
            'data_source': 'metabase'
        }
        
        with patch('uuid.uuid4', return_value=uuid.UUID('12345678-1234-5678-1234-567812345678')):
            with patch('datetime.datetime') as mock_datetime:
                mock_datetime.utcnow.return_value.isoformat.return_value = '2025-01-01T12:00:00Z'
                
                task_id = client.create_task(params)
        
        assert task_id == '12345678-1234-5678-1234-567812345678'
        
        expected_item = {
            'task_id': '12345678-1234-5678-1234-567812345678',
            'status': 'pending',
            'created_at': '2025-01-01T12:00:00Z',
            'updated_at': '2025-01-01T12:00:00Z',
            'parameters': params,
            'total_records': 0,
            'processed_records': 0,
            'errors': []
        }
        
        mock_table.put_item.assert_called_once_with(Item=expected_item)
    
    @patch('boto3.resource')
    def test_update_task_status_success(self, mock_resource):
        """Test successful task status update."""
        mock_table = Mock()
        mock_resource.return_value.Table.return_value = mock_table
        
        client = DynamoDBClient('test-table')
        
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value.isoformat.return_value = '2025-01-01T13:00:00Z'
            
            client.update_task_status(
                'test-task-123',
                'completed',
                total_records=100,
                processed_records=100
            )
        
        mock_table.update_item.assert_called_once()
        call_args = mock_table.update_item.call_args
        
        assert call_args[1]['Key'] == {'task_id': 'test-task-123'}
        assert 'status = :status' in call_args[1]['UpdateExpression']
        assert call_args[1]['ExpressionAttributeValues'][':status'] == 'completed'
        assert call_args[1]['ExpressionAttributeValues'][':total'] == 100
        assert call_args[1]['ExpressionAttributeValues'][':processed'] == 100
    
    @patch('boto3.resource')
    def test_update_task_status_with_error(self, mock_resource):
        """Test task status update with error."""
        mock_table = Mock()
        mock_resource.return_value.Table.return_value = mock_table
        
        client = DynamoDBClient('test-table')
        
        error_msg = "Test error message"
        
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value.isoformat.return_value = '2025-01-01T13:00:00Z'
            
            client.update_task_status(
                'test-task-123',
                'failed',
                error=error_msg
            )
        
        mock_table.update_item.assert_called_once()
        call_args = mock_table.update_item.call_args
        
        assert call_args[1]['ExpressionAttributeValues'][':errors'] == [error_msg]
    
    @patch('boto3.resource')
    def test_get_task(self, mock_resource):
        """Test getting a task."""
        mock_table = Mock()
        mock_resource.return_value.Table.return_value = mock_table
        
        mock_response = {
            'Item': {
                'task_id': 'test-task-123',
                'status': 'completed'
            }
        }
        mock_table.get_item.return_value = mock_response
        
        client = DynamoDBClient('test-table')
        
        task = client.get_task('test-task-123')
        
        assert task == mock_response['Item']
        mock_table.get_item.assert_called_once_with(Key={'task_id': 'test-task-123'})
    
    @patch('boto3.resource')
    def test_get_task_not_found(self, mock_resource):
        """Test getting a non-existent task."""
        mock_table = Mock()
        mock_resource.return_value.Table.return_value = mock_table
        
        mock_table.get_item.return_value = {}
        
        client = DynamoDBClient('test-table')
        
        task = client.get_task('non-existent')
        
        assert task is None
    
    @patch('boto3.resource')
    def test_create_task_with_chunks(self, mock_resource):
        """Test creating a task with chunk tracking."""
        mock_table = Mock()
        mock_resource.return_value.Table.return_value = mock_table
        
        client = DynamoDBClient('test-table')
        
        params = {
            'data_source': 's3_csv',
            's3_key': 'test.csv'
        }
        
        with patch('uuid.uuid4', return_value=uuid.UUID('12345678-1234-5678-1234-567812345678')):
            with patch('datetime.datetime') as mock_datetime:
                mock_datetime.utcnow.return_value.isoformat.return_value = '2025-01-01T12:00:00Z'
                
                task_id = client.create_task_with_chunks(params, total_chunks=5, total_rows=1000)
        
        assert task_id == '12345678-1234-5678-1234-567812345678'
        
        expected_item = {
            'task_id': '12345678-1234-5678-1234-567812345678',
            'status': 'pending',
            'created_at': '2025-01-01T12:00:00Z',
            'updated_at': '2025-01-01T12:00:00Z',
            'parameters': params,
            'total_records': 1000,
            'processed_records': 0,
            'errors': [],
            'total_chunks': 5,
            'chunks': {
                '0': {'status': 'pending', 'processed': 0, 'total': 0},
                '1': {'status': 'pending', 'processed': 0, 'total': 0},
                '2': {'status': 'pending', 'processed': 0, 'total': 0},
                '3': {'status': 'pending', 'processed': 0, 'total': 0},
                '4': {'status': 'pending', 'processed': 0, 'total': 0}
            }
        }
        
        mock_table.put_item.assert_called_once_with(Item=expected_item)
    
    @patch('boto3.resource')
    def test_update_chunk_status(self, mock_resource):
        """Test updating chunk status."""
        mock_table = Mock()
        mock_resource.return_value.Table.return_value = mock_table
        
        client = DynamoDBClient('test-table')
        
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value.isoformat.return_value = '2025-01-01T13:00:00Z'
            
            client.update_chunk_status(
                'test-task-123',
                2,
                'completed',
                processed=200,
                total=200
            )
        
        mock_table.update_item.assert_called_once()
        call_args = mock_table.update_item.call_args
        
        assert call_args[1]['Key'] == {'task_id': 'test-task-123'}
        assert 'chunks.#chunk_id' in call_args[1]['UpdateExpression']
        assert call_args[1]['ExpressionAttributeNames']['#chunk_id'] == '2'