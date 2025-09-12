import os
import time
import logging
import openai
from typing import List


logger = logging.getLogger(__name__)


class OpenAIClient:
    def __init__(self, rate_limit_per_second=5, timeout=30):
        openai.api_key = os.environ.get('OPENAI_API_KEY')
        self.rate_limit_per_second = rate_limit_per_second
        self.last_request_time = 0
        self.timeout = timeout  # Default 30 seconds timeout for OpenAI requests
        
        if not os.environ.get('OPENAI_API_KEY'):
            raise ValueError("Missing OPENAI_API_KEY environment variable")
    
    def _rate_limit(self):
        """Apply rate limiting to API calls."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        min_interval = 1.0 / self.rate_limit_per_second
        
        if time_since_last_request < min_interval:
            sleep_time = min_interval - time_since_last_request
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def get_embedding(self, text: str, max_retries: int = 3) -> List[float]:
        """
        Get embedding for a single text using OpenAI's text-embedding-ada-002 model.
        
        Args:
            text (str): Text to embed
            max_retries (int): Maximum number of retries for overloaded server
            
        Returns:
            List[float]: Embedding vector
        """
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                
                logger.debug(f"Getting embedding for text of length {len(text)} (attempt {attempt + 1}/{max_retries})")
                
                response = openai.Embedding.create(
                    model="text-embedding-ada-002",
                    input=text,
                    timeout=self.timeout  # Add explicit timeout
                )
                
                embedding = response['data'][0]['embedding']
                logger.debug(f"Successfully generated embedding of dimension {len(embedding)}")
                
                return embedding
                
            except Exception as e:
                error_str = str(e)
                # Check if it's an overload error
                if "overloaded" in error_str.lower() or "not ready" in error_str.lower():
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 2  # Exponential backoff: 2, 4, 8 seconds
                        logger.warning(f"OpenAI server overloaded, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                
                # Log more details about the error
                error_msg = f"Error generating embedding: {e}"
                if "timeout" in error_str.lower():
                    error_msg = f"OpenAI request timed out after {self.timeout}s: {e}"
                logger.error(error_msg)
                raise
    
    def get_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Get embeddings for multiple texts with rate limiting.
        
        Args:
            texts (List[str]): List of texts to embed
            
        Returns:
            List[List[float]]: List of embedding vectors
        """
        embeddings = []
        
        for i, text in enumerate(texts):
            try:
                embedding = self.get_embedding(text)
                embeddings.append(embedding)
                
                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i + 1}/{len(texts)} embeddings")
                    
            except Exception as e:
                logger.error(f"Failed to generate embedding for text {i}: {e}")
                # Continue processing other texts
                embeddings.append(None)
        
        return embeddings