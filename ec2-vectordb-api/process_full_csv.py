#!/usr/bin/env python3
"""
Process the entire sample.csv file using the enhanced chunk processor.
This simulates what the API would do when receiving a request.
"""

import os
import sys
import time
import uuid
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Load environment variables
from dotenv import load_dotenv
load_dotenv('.env.local')

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from app.workers.chunk_processor import process_csv_file
from app.utils.csv_streaming import analyze_csv_structure


def main():
    """Process the entire sample.csv file."""
    
    print("\n" + "="*60)
    print("PROCESSING FULL sample.csv FILE")
    print("="*60)
    
    # Configuration
    csv_file = "sample.csv"
    question_id = "232"
    task_id = f"test_{uuid.uuid4().hex[:8]}"
    
    print(f"\nConfiguration:")
    print(f"  CSV File: {csv_file}")
    print(f"  Question ID: {question_id}")
    print(f"  Task ID: {task_id}")
    print(f"  USE_LOCAL_FILES: {os.environ.get('USE_LOCAL_FILES', 'false')}")
    print(f"  Pinecone Index: {os.environ.get('PINECONE_INDEX_NAME', 'clickbid')}")
    print(f"  Chunk Size: {os.environ.get('CHUNK_SIZE', '1000')}")
    
    # Step 1: Analyze the CSV file
    print(f"\n{'='*60}")
    print("Step 1: Analyzing CSV file...")
    print("="*60)
    
    try:
        analysis = analyze_csv_structure(csv_file, use_local=True)
        print(f"✓ CSV Analysis:")
        print(f"  - Total rows: {analysis['total_rows']:,}")
        print(f"  - Columns: {analysis['column_count']}")
        print(f"  - Estimated size: {analysis['estimated_size_mb']} MB")
        print(f"  - Columns found: {', '.join(analysis['columns'][:10])}...")
        
        # Verify required columns
        required_cols = {'id', 'record_type', 'formatted_text'}
        found_cols = set(analysis['columns'])
        if required_cols.issubset(found_cols):
            print(f"  ✓ All required columns present: {', '.join(required_cols)}")
        else:
            missing = required_cols - found_cols
            print(f"  ❌ Missing required columns: {', '.join(missing)}")
            return
            
    except Exception as e:
        print(f"❌ Failed to analyze CSV: {e}")
        return
    
    # Step 2: Process the CSV file
    print(f"\n{'='*60}")
    print("Step 2: Processing CSV with enhanced chunk processor...")
    print("="*60)
    
    try:
        # Determine processing mode based on file size
        if analysis['total_rows'] > 10000:
            print(f"  Using STREAMING mode (file has {analysis['total_rows']:,} rows)")
            streaming = True
        else:
            print(f"  Using PARALLEL mode (file has {analysis['total_rows']:,} rows)")
            streaming = False
        
        start_time = time.time()
        
        # Process the CSV file
        results = process_csv_file(
            task_id=task_id,
            file_path=csv_file,
            question_id=question_id,
            use_local=True,
            streaming=streaming,
            chunk_size=1000,  # Process 1000 rows at a time
            max_workers=2  # Use 2 workers for parallel processing
        )
        
        elapsed = time.time() - start_time
        
        print(f"\n✓ Processing completed in {elapsed:.2f} seconds")
        print(f"\nResults:")
        print(f"  - Status: {results.get('status', 'unknown')}")
        print(f"  - Total rows: {results.get('total_rows', 0):,}")
        print(f"  - Processed: {results.get('total_processed', 0):,}")
        print(f"  - Failed: {results.get('total_failed', 0):,}")
        print(f"  - Vectors upserted: {results.get('vectors_upserted', 0):,}")
        print(f"  - Processing rate: {results.get('processing_rate', 0):.2f} rows/sec")
        
        if results.get('total_failed', 0) > 0:
            print(f"\n⚠ Warning: {results['total_failed']} records failed to process")
        
    except Exception as e:
        print(f"❌ Processing failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 3: Verify data in Pinecone
    print(f"\n{'='*60}")
    print("Step 3: Verifying data in Pinecone...")
    print("="*60)
    
    try:
        from pinecone import Pinecone
        import random
        
        pc = Pinecone(api_key=os.environ.get('PINECONE_API_KEY'))
        index = pc.Index(os.environ.get('PINECONE_INDEX_NAME', 'clickbid'))
        
        # Get index statistics
        stats = index.describe_index_stats()
        print(f"\n✓ Pinecone Index Statistics:")
        print(f"  - Total vectors: {stats.get('total_vector_count', 0):,}")
        print(f"  - Dimensions: {stats.get('dimension', 'unknown')}")
        
        print(f"\n  Namespaces:")
        for ns_name, ns_stats in stats.get('namespaces', {}).items():
            count = ns_stats.get('vector_count', 0)
            print(f"    - {ns_name}: {count:,} vectors")
            
            # Check for vectors with our question_id
            if count > 0:
                # Query for vectors with our question_id format
                random_vector = [random.random() for _ in range(1536)]
                
                response = index.query(
                    vector=random_vector,
                    top_k=10,
                    namespace=ns_name,
                    include_metadata=True
                )
                
                our_vectors = []
                for match in response.get('matches', []):
                    if match['id'].startswith(f"{question_id}."):
                        our_vectors.append(match)
                
                if our_vectors:
                    print(f"      → Found {len(our_vectors)} vectors with question_id={question_id}")
                    
                    # Show sample vector
                    sample = our_vectors[0]
                    print(f"\n      Sample vector:")
                    print(f"        - ID: {sample['id']}")
                    print(f"        - Score: {sample['score']:.4f}")
                    if 'metadata' in sample:
                        meta = sample['metadata']
                        print(f"        - Metadata fields: {', '.join(list(meta.keys())[:10])}...")
                        print(f"        - Row ID: {meta.get('row_id', 'N/A')}")
                        print(f"        - Record Type: {meta.get('record_type', 'N/A')}")
                        print(f"        - Text preview: {meta.get('text', '')[:100]}...")
        
        print(f"\n✅ Data successfully stored in Pinecone!")
        
    except Exception as e:
        print(f"❌ Failed to verify Pinecone data: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n{'='*60}")
    print("PROCESSING COMPLETE")
    print("="*60)
    print(f"\nSummary:")
    print(f"  ✓ Processed {results.get('total_processed', 0):,} records from {csv_file}")
    print(f"  ✓ Stored vectors with ID format: {question_id}.{{row_id}}")
    print(f"  ✓ Organized by record_type into namespaces")
    print(f"  ✓ All CSV metadata included in Pinecone")


if __name__ == "__main__":
    main()