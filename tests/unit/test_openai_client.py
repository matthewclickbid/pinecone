"""Unit tests for OpenAI client."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import time

from services.openai_client import OpenAIClient

class TestOpenAIClient:
    """Test OpenAI client operations."""
    
    @patch('openai.OpenAI')
    def test_init(self, mock_openai):
        """Test OpenAIClient initialization."""
        mock_client = Mock()
        mock_openai.return_value = mock_client
        
        client = OpenAIClient('test-api-key', rate_limit_per_second=10)
        
        assert client.client == mock_client
        assert client.rate_limit_per_second == 10
        assert client.min_delay == 0.1
        mock_openai.assert_called_once_with(api_key='test-api-key')
    
    @patch('openai.OpenAI')
    @patch('time.sleep')
    def test_generate_embeddings_single_batch(self, mock_sleep, mock_openai):
        """Test embedding generation for a single batch."""
        mock_client = Mock()
        mock_openai.return_value = mock_client
        
        # Mock embedding response
        mock_embedding = Mock()
        mock_embedding.embedding = [0.1] * 1536
        mock_response = Mock()
        mock_response.data = [mock_embedding]
        mock_client.embeddings.create.return_value = mock_response
        
        client = OpenAIClient('test-api-key')
        
        texts = ["Test text 1"]
        embeddings = client.generate_embeddings(texts)
        
        assert len(embeddings) == 1
        assert len(embeddings[0]) == 1536
        assert embeddings[0][0] == 0.1
        
        mock_client.embeddings.create.assert_called_once_with(
            model="text-embedding-ada-002",
            input="Test text 1"
        )
    
    @patch('openai.OpenAI')
    @patch('time.sleep')
    def test_generate_embeddings_multiple_texts(self, mock_sleep, mock_openai):
        """Test embedding generation for multiple texts."""
        mock_client = Mock()
        mock_openai.return_value = mock_client
        
        # Mock multiple embedding responses
        embeddings_data = []
        for i in range(3):
            mock_embedding = Mock()
            mock_embedding.embedding = [0.1 * (i + 1)] * 1536
            embeddings_data.append(mock_embedding)
        
        mock_responses = []
        for embedding in embeddings_data:
            mock_response = Mock()
            mock_response.data = [embedding]
            mock_responses.append(mock_response)
        
        mock_client.embeddings.create.side_effect = mock_responses
        
        client = OpenAIClient('test-api-key', rate_limit_per_second=20)
        
        texts = ["Text 1", "Text 2", "Text 3"]
        embeddings = client.generate_embeddings(texts)
        
        assert len(embeddings) == 3
        assert embeddings[0][0] == 0.1
        assert embeddings[1][0] == 0.2
        assert embeddings[2][0] == 0.3
        
        assert mock_client.embeddings.create.call_count == 3
        # Should have called sleep for rate limiting
        assert mock_sleep.call_count >= 2
    
    @patch('openai.OpenAI')
    @patch('time.sleep')
    def test_generate_embeddings_with_rate_limiting(self, mock_sleep, mock_openai):
        """Test rate limiting in embedding generation."""
        mock_client = Mock()
        mock_openai.return_value = mock_client
        
        mock_embedding = Mock()
        mock_embedding.embedding = [0.1] * 1536
        mock_response = Mock()
        mock_response.data = [mock_embedding]
        mock_client.embeddings.create.return_value = mock_response
        
        # Set very low rate limit to test delay
        client = OpenAIClient('test-api-key', rate_limit_per_second=2)
        
        texts = ["Text 1", "Text 2"]
        
        with patch('time.time') as mock_time:
            # Simulate time passing
            mock_time.side_effect = [0, 0, 0.4, 0.4]
            embeddings = client.generate_embeddings(texts)
        
        assert len(embeddings) == 2
        # Should have called sleep due to rate limiting
        mock_sleep.assert_called()
    
    @patch('openai.OpenAI')
    def test_generate_embeddings_error_handling(self, mock_openai):
        """Test error handling in embedding generation."""
        mock_client = Mock()
        mock_openai.return_value = mock_client
        
        # First call fails, second succeeds
        mock_embedding = Mock()
        mock_embedding.embedding = [0.1] * 1536
        mock_response = Mock()
        mock_response.data = [mock_embedding]
        
        mock_client.embeddings.create.side_effect = [
            Exception("API Error"),
            mock_response
        ]
        
        client = OpenAIClient('test-api-key')
        
        texts = ["Text 1", "Text 2"]
        embeddings = client.generate_embeddings(texts)
        
        # Should have one None for the failed embedding
        assert len(embeddings) == 2
        assert embeddings[0] is None
        assert embeddings[1] is not None
        assert len(embeddings[1]) == 1536
    
    @patch('openai.OpenAI')
    def test_generate_embeddings_empty_list(self, mock_openai):
        """Test handling of empty text list."""
        mock_client = Mock()
        mock_openai.return_value = mock_client
        
        client = OpenAIClient('test-api-key')
        
        embeddings = client.generate_embeddings([])
        
        assert embeddings == []
        mock_client.embeddings.create.assert_not_called()
    
    @patch('openai.OpenAI')
    @patch('time.sleep')
    def test_generate_embeddings_sanitized_text(self, mock_sleep, mock_openai):
        """Test that text is properly sanitized before sending to API."""
        mock_client = Mock()
        mock_openai.return_value = mock_client
        
        mock_embedding = Mock()
        mock_embedding.embedding = [0.1] * 1536
        mock_response = Mock()
        mock_response.data = [mock_embedding]
        mock_client.embeddings.create.return_value = mock_response
        
        client = OpenAIClient('test-api-key')
        
        # Text with special characters that should be handled
        texts = ["Text with\nnewlines\tand\ttabs"]
        embeddings = client.generate_embeddings(texts)
        
        assert len(embeddings) == 1
        mock_client.embeddings.create.assert_called_once()
        
        # Verify the text was passed correctly
        call_args = mock_client.embeddings.create.call_args
        assert call_args[1]['input'] == texts[0]