import os
import logging
from pinecone import Pinecone
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class PineconeClient:
    def __init__(self):
        self.api_key = os.environ.get('PINECONE_API_KEY')
        self.environment = os.environ.get('PINECONE_ENVIRONMENT')  # e.g. 'us-east-1'
        self.index_name = os.environ.get('PINECONE_INDEX_NAME')

        if not all([self.api_key, self.environment, self.index_name]):
            raise ValueError("Missing one or more Pinecone environment variables")

        try:
            logger.info(f"Initializing Pinecone v3 client with index: {self.index_name}")
            self.pinecone = Pinecone(api_key=self.api_key, environment=self.environment)
            self.index = self.pinecone.Index(self.index_name)
            logger.info(f"Connected to Pinecone index: {self.index_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Pinecone client: {e}")
            raise

    def upsert_vectors(self, vectors_data: List[Dict[str, Any]], batch_size: int = 100) -> Dict[str, Any]:
        """
        Upsert vectors to the Pinecone index in batches to avoid size limits.
        Each item in vectors_data should have 'id', 'values', and optional 'metadata'.
        
        Args:
            vectors_data: List of vector dictionaries to upsert
            batch_size: Number of vectors to process per batch (default: 100)
        
        Returns:
            Aggregated response from all batch operations
        """
        try:
            total_vectors = len(vectors_data)
            logger.info(f"Starting upsert: {total_vectors} vectors to index '{self.index_name}' in batches of {batch_size}")
            
            if total_vectors == 0:
                logger.warning("No vectors provided for upsert - returning early")
                return {'upserted_count': 0}
            
            # Log sample vector structure for debugging
            if vectors_data:
                sample_vector = vectors_data[0]
                logger.info(f"Sample vector structure: id='{sample_vector.get('id')}', values_length={len(sample_vector.get('values', []))}, metadata_keys={list(sample_vector.get('metadata', {}).keys())}")
            
            # Process vectors in batches
            total_upserted = 0
            responses = []
            
            for i in range(0, total_vectors, batch_size):
                batch = vectors_data[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (total_vectors + batch_size - 1) // batch_size
                
                logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} vectors)")
                logger.info(f"Batch {batch_num} vector IDs: {[v.get('id') for v in batch[:3]]}{'...' if len(batch) > 3 else ''}")
                
                try:
                    logger.info(f"Calling Pinecone index.upsert() for batch {batch_num}")
                    response = self.index.upsert(vectors=batch)
                    logger.info(f"Batch {batch_num} Pinecone response: {response}")
                    
                    responses.append(response)
                    
                    # Count upserted vectors from this batch
                    batch_upserted = response.get('upserted_count', len(batch))
                    total_upserted += batch_upserted
                    
                    logger.info(f"Batch {batch_num} SUCCESS: {batch_upserted} vectors upserted (running total: {total_upserted})")
                    
                except Exception as batch_error:
                    logger.error(f"Batch {batch_num} FAILED: {type(batch_error).__name__}: {batch_error}")
                    logger.error(f"Failed batch sample vector: {batch[0] if batch else 'No vectors in batch'}")
                    raise
            
            # Return aggregated response
            aggregated_response = {
                'upserted_count': total_upserted,
                'total_batches': len(responses),
                'batch_responses': responses
            }
            
            logger.info(f"UPSERT COMPLETE: {total_upserted} total vectors successfully upserted to Pinecone in {len(responses)} batches")
            return aggregated_response
            
        except Exception as e:
            logger.error(f"Upsert failed: {e}")
            raise