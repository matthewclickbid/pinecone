import os
import time
import logging
import openai
from typing import List


logger = logging.getLogger(__name__)


class OpenAIClient:
    def __init__(self, rate_limit_per_second=10):
        openai.api_key = os.environ.get('OPENAI_API_KEY')
        self.rate_limit_per_second = rate_limit_per_second
        self.last_request_time = 0
        
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
    
    def get_embedding(self, text: str) -> List[float]:
        """
        Get embedding for a single text using OpenAI's text-embedding-ada-002 model.
        
        Args:
            text (str): Text to embed
            
        Returns:
            List[float]: Embedding vector
        """
        try:
            self._rate_limit()
            
            logger.debug(f"Getting embedding for text of length {len(text)}")
            
            response = openai.Embedding.create(
                model="text-embedding-ada-002",
                input=text
            )
            
            embedding = response['data'][0]['embedding']
            logger.debug(f"Successfully generated embedding of dimension {len(embedding)}")
            
            return embedding
            
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
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