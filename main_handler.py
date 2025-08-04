import json
import logging
import os
import boto3
from datetime import datetime, timedelta
from typing import Dict, Any

from services.dynamodb_client import DynamoDBClient
from utils.date_utils import calculate_end_date, parse_date

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_yesterday_date():
    """
    Get yesterday's date in YYYY-MM-DD format.
    
    Returns:
        str: Yesterday's date in YYYY-MM-DD format
    """
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')


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
        
        # Get start_date parameter or default to yesterday
        start_date = query_params.get('start_date')
        if not start_date:
            start_date = get_yesterday_date()
            logger.info(f"No start_date provided, using yesterday: {start_date}")
        
        # Parse and validate date format
        try:
            parse_date(start_date)
        except ValueError as e:
            return create_response(400, {'error': str(e)})
        
        # Get optional parameters
        question_id = query_params.get('question_id')
        data_source = query_params.get('data_source', 'metabase')  # Default to metabase
        s3_key = query_params.get('s3_key')
        date_column = query_params.get('date_column', 'created_at')
        use_step_functions = query_params.get('use_step_functions', 'false').lower() == 'true'
        
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
        logger.info(f"Step Functions ARN fix - v4")
        
        # Handle date parameters based on data source
        if data_source == 'metabase':
            # Calculate end date - if start_date was defaulted to yesterday, use today as end_date
            if query_params.get('start_date'):
                # User provided start_date, use normal +2 days logic
                end_date = calculate_end_date(start_date)
            else:
                # start_date was defaulted to yesterday, use today as end_date
                end_date = datetime.now().strftime('%Y-%m-%d')
        else:  # s3_csv
            # For S3 CSV, we process the entire file regardless of dates
            # Set dummy dates for compatibility with existing task management
            from datetime import datetime
            today = datetime.now().strftime('%Y-%m-%d')
            start_date = today
            end_date = today
            logger.info("S3 CSV mode: Processing entire file, ignoring date parameters")
        
        logger.info(f"Processing request for dates {start_date} to {end_date}")
        
        # Initialize DynamoDB client
        dynamodb_client = DynamoDBClient()
        
        # Create task in DynamoDB
        task_id = dynamodb_client.create_task(start_date, end_date)
        
        # Determine processing method: Step Functions for large S3 CSV files or regular Lambda
        if data_source == 's3_csv' and use_step_functions:
            # Use Step Functions for large CSV processing
            logger.info(f"Using Step Functions workflow for large CSV processing")
            
            stepfunctions_client = boto3.client('stepfunctions', region_name='us-east-1')
            
            # Prepare Step Functions input
            step_input = {
                'task_id': task_id,
                's3_key': s3_key,
                'question_id': question_id,
                'start_date': start_date,
                'end_date': end_date,
                'bucket_name': os.environ.get('S3_BUCKET_NAME', 'pineconeuploads')
            }
            
            # Start Step Functions execution
            state_machine_arn = os.environ.get('STEP_FUNCTIONS_ARN', 
                f"arn:aws:states:us-east-1:267578238439:stateMachine:vectordb-csv-processing-dev")
            
            try:
                execution_response = stepfunctions_client.start_execution(
                    stateMachineArn=state_machine_arn,
                    name=f"csv-processing-{task_id}",
                    input=json.dumps(step_input)
                )
                
                execution_arn = execution_response['executionArn']
                logger.info(f"Started Step Functions execution: {execution_arn}")
                
                # Update task with execution info
                dynamodb_client.update_task_status(
                    task_id, 
                    'STEP_FUNCTIONS_STARTED',
                    step_functions_execution_arn=execution_arn
                )
                
            except Exception as e:
                error_msg = f"Failed to start Step Functions execution: {e}"
                logger.error(error_msg)
                dynamodb_client.set_task_error(task_id, error_msg)
                return create_response(500, {'error': error_msg})
        
        else:
            # Use regular Lambda async processing (existing method)
            logger.info(f"Using regular Lambda async processing")
            
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