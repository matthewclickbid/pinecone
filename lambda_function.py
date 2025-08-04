import json
import logging
import os
from typing import Dict, Any

from services.metabase_client import MetabaseClient
from services.openai_client import OpenAIClient
from services.pinecone_client import PineconeClient
from services.dynamodb_client import DynamoDBClient
from utils.date_utils import calculate_end_date, parse_date
from utils.text_sanitizer import sanitize_text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler for vectorizing Metabase data and storing in Pinecone.
    
    Args:
        event: Lambda event containing API Gateway request
        context: Lambda context object
        
    Returns:
        Dict: API response
    """
    try:
        # Parse request
        request_method = event.get('httpMethod', 'GET')
        query_params = event.get('queryStringParameters') or {}
        headers = event.get('headers', {})
        
        # Validate HTTP method
        if request_method != 'GET':
            return create_response(405, {'error': 'Method not allowed'})
        
        # Validate API key
        api_key = headers.get('x-api-key')
        expected_api_key = os.environ.get('API_KEY')
        
        if not api_key or api_key != expected_api_key:
            return create_response(401, {'error': 'Unauthorized'})
        
        # Get optional parameters
        question_id = query_params.get('question_id')
        data_source = query_params.get('data_source', 'metabase')  # Default to metabase
        s3_key = query_params.get('s3_key')
        date_column = query_params.get('date_column', 'created_at')
        
        # Validate data_source
        if data_source not in ['metabase', 's3_csv']:
            return create_response(400, {'error': 'Invalid data_source. Must be "metabase" or "s3_csv"'})
        
        # Validate required parameters for each data source
        if data_source == 'metabase':
            if question_id:
                logger.info(f"Using custom question_id: {question_id}")
            else:
                logger.info("Using default question_id from environment variable")
        elif data_source == 's3_csv':
            if not s3_key:
                return create_response(400, {'error': 's3_key is required when data_source is "s3_csv"'})
            if not question_id:
                return create_response(400, {'error': 'question_id is required when data_source is "s3_csv" for vector ID generation'})
            logger.info(f"Using S3 CSV source: {s3_key}")
            logger.info(f"Using question_id for vector IDs: {question_id}")
            logger.info(f"Date filtering column: {date_column}")  # Still logged but not used for filtering
        
        logger.info(f"Data source: {data_source}")
        
        # Handle date parameters based on data source
        if data_source == 'metabase':
            # Validate start_date parameter for Metabase
            start_date = query_params.get('start_date')
            if not start_date:
                return create_response(400, {'error': 'start_date parameter is required for Metabase data source'})
            
            # Parse and validate date format
            try:
                parse_date(start_date)
            except ValueError as e:
                return create_response(400, {'error': str(e)})
            
            # Calculate end date
            end_date = calculate_end_date(start_date)
        else:  # s3_csv
            # For S3 CSV, we process the entire file regardless of dates
            # Set dummy dates for compatibility with existing task management
            from datetime import datetime
            today = datetime.now().strftime('%Y-%m-%d')
            start_date = today
            end_date = today
            logger.info("S3 CSV mode: Processing entire file, ignoring date parameters")
        
        logger.info(f"Processing request for dates {start_date} to {end_date}")
        logger.info(f"Start date: '{start_date}', End date: '{end_date}'")
        
        # Initialize DynamoDB client only (other clients initialized in async handler)
        dynamodb_client = DynamoDBClient()
        
        # Create task in DynamoDB
        task_id = dynamodb_client.create_task(start_date, end_date)
        
        # Trigger async processing
        import boto3
        lambda_client = boto3.client('lambda')
        
        # Invoke async processing function
        async_payload = {
            'task_id': task_id,
            'start_date': start_date,
            'end_date': end_date,
            'data_source': data_source
        }
        
        # Add optional parameters to payload
        if question_id:
            async_payload['question_id'] = question_id
        if data_source == 's3_csv':
            async_payload['s3_key'] = s3_key
            async_payload['date_column'] = date_column
        
        lambda_client.invoke(
            FunctionName=os.environ.get('ASYNC_FUNCTION_NAME', 'vectordb-processing-async-dev'),
            InvocationType='Event',  # Async invocation
            Payload=json.dumps(async_payload)
        )
        
        # Return immediately with task ID for async processing
        response_data = {
            'message': 'Processing started',
            'task_id': task_id,
            'start_date': start_date,
            'end_date': end_date,
            'data_source': data_source,
            'status': 'PENDING',
            'status_url': f"/status?task_id={task_id}"
        }
        
        # Add source-specific info to response
        if data_source == 's3_csv':
            response_data['s3_key'] = s3_key
            response_data['date_column'] = date_column
        elif question_id:
            response_data['question_id'] = question_id
        
        return create_response(202, response_data)
            
    except Exception as e:
        logger.error(f"Error starting async processing: {e}")
        return create_response(500, {'error': 'Failed to start processing'})


def async_process_handler(event, context):
    """
    Handler for async processing of Metabase data.
    """
    try:
        task_id = event.get('task_id')
        start_date = event.get('start_date')
        end_date = event.get('end_date')
        
        logger.info(f"Starting async processing for task {task_id}")
        
        # Initialize clients
        metabase_client = MetabaseClient()
        openai_client = OpenAIClient(rate_limit_per_second=20)
        pinecone_client = PineconeClient()
        dynamodb_client = DynamoDBClient()
        
        try:
            # Update task status to IN_PROGRESS
            dynamodb_client.update_task_status(task_id, 'IN_PROGRESS')
            
            # Fetch data from Metabase
            logger.info("Fetching data from Metabase...")
            metabase_data = metabase_client.fetch_question_results(start_date, end_date)
            
            if not metabase_data:
                logger.warning("No data returned from Metabase")
                dynamodb_client.update_task_status(task_id, 'COMPLETED', total_records=0)
                return
            
            # Update total records count
            total_records = len(metabase_data)
            dynamodb_client.update_task_status(task_id, 'IN_PROGRESS', total_records=total_records)
            
            logger.info(f"Processing {total_records} records")
            
            # Process each record
            processed_count = 0
            failed_count = 0
            vectors_to_upsert = []
            
            for i, record in enumerate(metabase_data):
                try:
                    # Extract required fields
                    record_id = record.get('id')
                    event_id = record.get('event_id')
                    org_id = record.get('org_id')
                    formatted_text = record.get('formatted_text')
                    
                    # Validate required fields
                    if not all([record_id, event_id, org_id, formatted_text]):
                        logger.warning(f"Record {i} missing required fields: {record}")
                        failed_count += 1
                        continue
                    
                    # Sanitize text
                    sanitized_text = sanitize_text(formatted_text)
                    
                    if not sanitized_text:
                        logger.warning(f"Record {i} has empty text after sanitization")
                        failed_count += 1
                        continue
                    
                    # Generate embedding
                    embedding = openai_client.get_embedding(sanitized_text)
                    
                    # Prepare vector data for Pinecone
                    # Start with all Metabase record fields
                    metadata = dict(record)
                    
                    # Remove fields that have special handling
                    metadata.pop('id', None)  # Used for vector ID
                    metadata.pop('formatted_text', None)  # Becomes 'text'
                    
                    # Add processed text field
                    metadata['text'] = sanitized_text
                    
                    # Add processing metadata
                    metadata.update({
                        'start_date': start_date,
                        'end_date': end_date,
                        'task_id': task_id
                    })
                    
                    # Convert all values to strings for Pinecone compatibility
                    metadata = {k: str(v) for k, v in metadata.items()}
                    
                    vector_data = {
                        'id': str(record_id),
                        'values': embedding,
                        'metadata': metadata
                    }
                    
                    vectors_to_upsert.append(vector_data)
                    processed_count += 1
                    
                    # Update progress
                    dynamodb_client.increment_processed_records(task_id)
                    
                    # Log progress
                    if processed_count % 10 == 0:
                        logger.info(f"Processed {processed_count}/{total_records} records")
                    
                except Exception as e:
                    logger.error(f"Error processing record {i}: {e}")
                    failed_count += 1
                    dynamodb_client.increment_failed_records(task_id)
            
            # Upsert vectors to Pinecone
            if vectors_to_upsert:
                logger.info(f"Upserting {len(vectors_to_upsert)} vectors to Pinecone")
                pinecone_response = pinecone_client.upsert_vectors(vectors_to_upsert)
                logger.info(f"Pinecone upsert completed: {pinecone_response}")
            
            # Update task status to completed
            dynamodb_client.update_task_status(
                task_id, 
                'COMPLETED',
                processed_records=processed_count,
                failed_records=failed_count
            )
            
            logger.info(f"Async processing completed for task {task_id}")
            
        except Exception as e:
            logger.error(f"Error during async processing: {e}")
            dynamodb_client.set_task_error(task_id, str(e))
            raise
            
    except Exception as e:
        logger.error(f"Async processing error: {e}")
        return


def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create API Gateway response.
    
    Args:
        status_code: HTTP status code
        body: Response body
        
    Returns:
        Dict: API Gateway response
    """
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, x-api-key'
        },
        'body': json.dumps(body)
    }


def get_task_status(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for checking task status.
    
    Args:
        event: Lambda event containing task_id
        context: Lambda context object
        
    Returns:
        Dict: Task status response
    """
    try:
        query_params = event.get('queryStringParameters') or {}
        task_id = query_params.get('task_id')
        
        if not task_id:
            return create_response(400, {'error': 'task_id parameter is required'})
        
        dynamodb_client = DynamoDBClient()
        task = dynamodb_client.get_task(task_id)
        
        if not task:
            return create_response(404, {'error': 'Task not found'})
        
        return create_response(200, task)
        
    except Exception as e:
        logger.error(f"Error getting task status: {e}")
        return create_response(500, {'error': 'Internal server error'})