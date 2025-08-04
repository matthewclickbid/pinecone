import json
import logging
from typing import Dict, Any
from services.dynamodb_client import DynamoDBClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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