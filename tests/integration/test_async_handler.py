"""Integration tests for async Lambda handler."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import json
import os

from async_handler import handler

class TestAsyncHandler:
    """Test async Lambda handler integration."""
    
    @patch.dict(os.environ, {
        'METABASE_URL': 'https://metabase.example.com',
        'METABASE_API_KEY': 'test-metabase-key',
        'METABASE_QUESTION_ID': '123',
        'OPENAI_API_KEY': 'test-openai-key',
        'PINECONE_API_KEY': 'test-pinecone-key',
        'PINECONE_INDEX_NAME': 'test-index',
        'PINECONE_NAMESPACE': 'test-namespace',
        'DYNAMODB_TABLE_NAME': 'test-table'
    })
    @patch('async_handler.DynamoDBClient')
    @patch('async_handler.MetabaseClient')
    @patch('async_handler.OpenAIClient')
    @patch('async_handler.PineconeClient')
    def test_process_metabase_data_success(self, mock_pc_class, mock_oai_class, 
                                          mock_mb_class, mock_db_class):
        """Test successful Metabase data processing."""
        # Setup mocks
        mock_db = Mock()
        mock_db_class.return_value = mock_db
        
        mock_mb = Mock()
        mock_mb.fetch_data.return_value = [
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
        mock_mb_class.return_value = mock_mb
        
        mock_oai = Mock()
        mock_oai.generate_embeddings.return_value = [
            [0.1] * 1536,
            [0.2] * 1536
        ]
        mock_oai_class.return_value = mock_oai
        
        mock_pc = Mock()
        mock_pc.prepare_vectors.return_value = [
            {'id': 'vec1', 'values': [0.1] * 1536, 'metadata': {}},
            {'id': 'vec2', 'values': [0.2] * 1536, 'metadata': {}}
        ]
        mock_pc_class.return_value = mock_pc
        
        # Create event
        event = {
            'task_id': 'test-task-123',
            'start_date': '2025-01-01',
            'end_date': '2025-01-02',
            'question_id': '123',
            'data_source': 'metabase'
        }
        
        result = handler(event, {})
        
        assert result['statusCode'] == 200
        assert result['task_id'] == 'test-task-123'
        assert result['status'] == 'completed'
        assert result['processed_records'] == 2
        
        # Verify service calls
        mock_mb.fetch_data.assert_called_once_with(
            '123', '2025-01-01', '2025-01-02'
        )
        mock_oai.generate_embeddings.assert_called_once()
        mock_pc.upsert_vectors.assert_called_once()
        
        # Verify status updates
        assert mock_db.update_task_status.call_count >= 2
        final_call = mock_db.update_task_status.call_args_list[-1]
        assert final_call[0][1] == 'completed'
        assert final_call[1]['processed_records'] == 2
    
    @patch.dict(os.environ, {
        'S3_BUCKET_NAME': 'test-bucket',
        'OPENAI_API_KEY': 'test-openai-key',
        'PINECONE_API_KEY': 'test-pinecone-key',
        'PINECONE_INDEX_NAME': 'test-index',
        'PINECONE_NAMESPACE': 'test-namespace',
        'DYNAMODB_TABLE_NAME': 'test-table'
    })
    @patch('async_handler.DynamoDBClient')
    @patch('async_handler.S3CSVClient')
    @patch('async_handler.OpenAIClient')
    @patch('async_handler.PineconeClient')
    def test_process_s3_csv_success(self, mock_pc_class, mock_oai_class,
                                   mock_s3_class, mock_db_class):
        """Test successful S3 CSV processing."""
        # Setup mocks
        mock_db = Mock()
        mock_db_class.return_value = mock_db
        
        mock_s3 = Mock()
        mock_s3.read_csv.return_value = [
            {
                'user_id': 'user1',
                'datetime': '2025-01-01T10:00:00Z',
                'content': 'CSV content 1'
            },
            {
                'user_id': 'user2',
                'datetime': '2025-01-01T11:00:00Z',
                'content': 'CSV content 2'
            }
        ]
        mock_s3_class.return_value = mock_s3
        
        mock_oai = Mock()
        mock_oai.generate_embeddings.return_value = [
            [0.1] * 1536,
            [0.2] * 1536
        ]
        mock_oai_class.return_value = mock_oai
        
        mock_pc = Mock()
        mock_pc.prepare_vectors.return_value = [
            {'id': 'vec1', 'values': [0.1] * 1536, 'metadata': {}},
            {'id': 'vec2', 'values': [0.2] * 1536, 'metadata': {}}
        ]
        mock_pc_class.return_value = mock_pc
        
        # Create event
        event = {
            'task_id': 'test-task-123',
            'data_source': 's3_csv',
            's3_key': 'data/test.csv'
        }
        
        result = handler(event, {})
        
        assert result['statusCode'] == 200
        assert result['status'] == 'completed'
        assert result['processed_records'] == 2
        
        # Verify S3 read
        mock_s3.read_csv.assert_called_once_with('data/test.csv')
    
    @patch.dict(os.environ, {
        'METABASE_URL': 'https://metabase.example.com',
        'METABASE_API_KEY': 'test-metabase-key',
        'DYNAMODB_TABLE_NAME': 'test-table'
    })
    @patch('async_handler.DynamoDBClient')
    @patch('async_handler.MetabaseClient')
    def test_process_with_metabase_error(self, mock_mb_class, mock_db_class):
        """Test handling of Metabase API error."""
        # Setup mocks
        mock_db = Mock()
        mock_db_class.return_value = mock_db
        
        mock_mb = Mock()
        mock_mb.fetch_data.side_effect = Exception("Metabase API error")
        mock_mb_class.return_value = mock_mb
        
        event = {
            'task_id': 'test-task-123',
            'start_date': '2025-01-01',
            'question_id': '123',
            'data_source': 'metabase'
        }
        
        result = handler(event, {})
        
        assert result['statusCode'] == 500
        assert result['status'] == 'failed'
        assert 'Metabase API error' in result['error']
        
        # Verify error status update
        mock_db.update_task_status.assert_called()
        final_call = mock_db.update_task_status.call_args_list[-1]
        assert final_call[0][1] == 'failed'
        assert 'error' in final_call[1]
    
    @patch.dict(os.environ, {
        'METABASE_URL': 'https://metabase.example.com',
        'METABASE_API_KEY': 'test-metabase-key',
        'OPENAI_API_KEY': 'test-openai-key',
        'PINECONE_API_KEY': 'test-pinecone-key',
        'PINECONE_INDEX_NAME': 'test-index',
        'PINECONE_NAMESPACE': 'test-namespace',
        'DYNAMODB_TABLE_NAME': 'test-table'
    })
    @patch('async_handler.DynamoDBClient')
    @patch('async_handler.MetabaseClient')
    @patch('async_handler.OpenAIClient')
    @patch('async_handler.PineconeClient')
    def test_process_with_partial_embedding_failures(self, mock_pc_class, mock_oai_class,
                                                    mock_mb_class, mock_db_class):
        """Test handling of partial embedding generation failures."""
        # Setup mocks
        mock_db = Mock()
        mock_db_class.return_value = mock_db
        
        mock_mb = Mock()
        mock_mb.fetch_data.return_value = [
            {'user_id': 'user1', 'datetime': '2025-01-01T10:00:00Z', 'content': 'Text 1'},
            {'user_id': 'user2', 'datetime': '2025-01-01T11:00:00Z', 'content': 'Text 2'},
            {'user_id': 'user3', 'datetime': '2025-01-01T12:00:00Z', 'content': 'Text 3'}
        ]
        mock_mb_class.return_value = mock_mb
        
        mock_oai = Mock()
        # Second embedding fails
        mock_oai.generate_embeddings.return_value = [
            [0.1] * 1536,
            None,  # Failed embedding
            [0.3] * 1536
        ]
        mock_oai_class.return_value = mock_oai
        
        mock_pc = Mock()
        mock_pc.prepare_vectors.return_value = [
            {'id': 'vec1', 'values': [0.1] * 1536, 'metadata': {}},
            {'id': 'vec3', 'values': [0.3] * 1536, 'metadata': {}}
        ]
        mock_pc_class.return_value = mock_pc
        
        event = {
            'task_id': 'test-task-123',
            'start_date': '2025-01-01',
            'question_id': '123',
            'data_source': 'metabase'
        }
        
        result = handler(event, {})
        
        # Should complete but with only 2 processed records
        assert result['statusCode'] == 200
        assert result['status'] == 'completed'
        assert result['processed_records'] == 2  # Only 2 successful
        
        # Verify vectors were prepared without the failed embedding
        mock_pc.prepare_vectors.assert_called_once()
        vectors_call = mock_pc.prepare_vectors.call_args[0]
        assert len(vectors_call[1]) == 3  # All embeddings passed
        assert vectors_call[1][1] is None  # Second one is None
    
    def test_missing_required_params(self):
        """Test handling of missing required parameters."""
        event = {}  # Missing task_id
        
        result = handler(event, {})
        
        assert result['statusCode'] == 400
        assert result['status'] == 'failed'
        assert 'Missing required' in result['error']