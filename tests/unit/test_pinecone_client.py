"""Unit tests for Pinecone client."""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime

from services.pinecone_client import PineconeClient

class TestPineconeClient:
    """Test Pinecone client operations."""
    
    @patch('pinecone.Pinecone')
    def test_init(self, mock_pinecone_class):
        """Test PineconeClient initialization."""
        mock_pinecone = Mock()
        mock_index = Mock()
        mock_pinecone.Index.return_value = mock_index
        mock_pinecone_class.return_value = mock_pinecone
        
        client = PineconeClient('test-api-key', 'test-index', 'test-namespace')
        
        assert client.index == mock_index
        assert client.namespace == 'test-namespace'
        assert client.batch_size == 100
        
        mock_pinecone_class.assert_called_once_with(api_key='test-api-key')
        mock_pinecone.Index.assert_called_once_with('test-index')
    
    @patch('pinecone.Pinecone')
    def test_upsert_vectors_single_batch(self, mock_pinecone_class):
        """Test upserting vectors in a single batch."""
        mock_pinecone = Mock()
        mock_index = Mock()
        mock_pinecone.Index.return_value = mock_index
        mock_pinecone_class.return_value = mock_pinecone
        
        client = PineconeClient('test-api-key', 'test-index', 'test-namespace')
        
        vectors = [
            {
                'id': 'vec1',
                'values': [0.1] * 1536,
                'metadata': {'text': 'Text 1'}
            },
            {
                'id': 'vec2',
                'values': [0.2] * 1536,
                'metadata': {'text': 'Text 2'}
            }
        ]
        
        client.upsert_vectors(vectors)
        
        mock_index.upsert.assert_called_once_with(
            vectors=vectors,
            namespace='test-namespace'
        )
    
    @patch('pinecone.Pinecone')
    def test_upsert_vectors_multiple_batches(self, mock_pinecone_class):
        """Test upserting vectors in multiple batches."""
        mock_pinecone = Mock()
        mock_index = Mock()
        mock_pinecone.Index.return_value = mock_index
        mock_pinecone_class.return_value = mock_pinecone
        
        client = PineconeClient('test-api-key', 'test-index', 'test-namespace', batch_size=2)
        
        vectors = [
            {'id': f'vec{i}', 'values': [0.1 * i] * 1536, 'metadata': {'text': f'Text {i}'}}
            for i in range(5)
        ]
        
        client.upsert_vectors(vectors)
        
        # Should have been called 3 times (2 + 2 + 1)
        assert mock_index.upsert.call_count == 3
        
        # Check the batches
        calls = mock_index.upsert.call_args_list
        assert len(calls[0][1]['vectors']) == 2  # First batch
        assert len(calls[1][1]['vectors']) == 2  # Second batch
        assert len(calls[2][1]['vectors']) == 1  # Last batch
        
        # All calls should use the namespace
        for call_args in calls:
            assert call_args[1]['namespace'] == 'test-namespace'
    
    @patch('pinecone.Pinecone')
    def test_upsert_vectors_empty_list(self, mock_pinecone_class):
        """Test handling of empty vector list."""
        mock_pinecone = Mock()
        mock_index = Mock()
        mock_pinecone.Index.return_value = mock_index
        mock_pinecone_class.return_value = mock_pinecone
        
        client = PineconeClient('test-api-key', 'test-index', 'test-namespace')
        
        client.upsert_vectors([])
        
        mock_index.upsert.assert_not_called()
    
    @patch('pinecone.Pinecone')
    def test_prepare_vectors(self, mock_pinecone_class):
        """Test vector preparation."""
        mock_pinecone = Mock()
        mock_index = Mock()
        mock_pinecone.Index.return_value = mock_index
        mock_pinecone_class.return_value = mock_pinecone
        
        client = PineconeClient('test-api-key', 'test-index', 'test-namespace')
        
        data = [
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
        
        embeddings = [
            [0.1] * 1536,
            [0.2] * 1536
        ]
        
        vectors = client.prepare_vectors(data, embeddings)
        
        assert len(vectors) == 2
        
        # Check first vector
        assert vectors[0]['id'] == 'user1_2025-01-01T10:00:00Z'
        assert vectors[0]['values'] == embeddings[0]
        assert vectors[0]['metadata']['user_id'] == 'user1'
        assert vectors[0]['metadata']['datetime'] == '2025-01-01T10:00:00Z'
        assert vectors[0]['metadata']['content'] == 'Test content 1'
        
        # Check second vector
        assert vectors[1]['id'] == 'user2_2025-01-01T11:00:00Z'
        assert vectors[1]['values'] == embeddings[1]
        assert vectors[1]['metadata']['user_id'] == 'user2'
    
    @patch('pinecone.Pinecone')
    def test_prepare_vectors_with_none_embeddings(self, mock_pinecone_class):
        """Test vector preparation with None embeddings."""
        mock_pinecone = Mock()
        mock_index = Mock()
        mock_pinecone.Index.return_value = mock_index
        mock_pinecone_class.return_value = mock_pinecone
        
        client = PineconeClient('test-api-key', 'test-index', 'test-namespace')
        
        data = [
            {'user_id': 'user1', 'datetime': '2025-01-01T10:00:00Z', 'content': 'Text 1'},
            {'user_id': 'user2', 'datetime': '2025-01-01T11:00:00Z', 'content': 'Text 2'},
            {'user_id': 'user3', 'datetime': '2025-01-01T12:00:00Z', 'content': 'Text 3'}
        ]
        
        embeddings = [
            [0.1] * 1536,
            None,  # Failed embedding
            [0.3] * 1536
        ]
        
        vectors = client.prepare_vectors(data, embeddings)
        
        # Should only have 2 vectors (skipping the None)
        assert len(vectors) == 2
        assert vectors[0]['id'] == 'user1_2025-01-01T10:00:00Z'
        assert vectors[1]['id'] == 'user3_2025-01-01T12:00:00Z'
    
    @patch('pinecone.Pinecone')
    def test_query_vectors(self, mock_pinecone_class):
        """Test querying vectors."""
        mock_pinecone = Mock()
        mock_index = Mock()
        mock_pinecone.Index.return_value = mock_index
        mock_pinecone_class.return_value = mock_pinecone
        
        # Mock query response
        mock_response = {
            'matches': [
                {'id': 'vec1', 'score': 0.9, 'metadata': {'text': 'Match 1'}},
                {'id': 'vec2', 'score': 0.8, 'metadata': {'text': 'Match 2'}}
            ]
        }
        mock_index.query.return_value = mock_response
        
        client = PineconeClient('test-api-key', 'test-index', 'test-namespace')
        
        query_vector = [0.1] * 1536
        results = client.query(query_vector, top_k=5)
        
        assert results == mock_response
        
        mock_index.query.assert_called_once_with(
            vector=query_vector,
            top_k=5,
            namespace='test-namespace',
            include_metadata=True
        )
    
    @patch('pinecone.Pinecone')
    def test_delete_vectors(self, mock_pinecone_class):
        """Test deleting vectors."""
        mock_pinecone = Mock()
        mock_index = Mock()
        mock_pinecone.Index.return_value = mock_index
        mock_pinecone_class.return_value = mock_pinecone
        
        client = PineconeClient('test-api-key', 'test-index', 'test-namespace')
        
        ids_to_delete = ['vec1', 'vec2', 'vec3']
        client.delete(ids_to_delete)
        
        mock_index.delete.assert_called_once_with(
            ids=ids_to_delete,
            namespace='test-namespace'
        )