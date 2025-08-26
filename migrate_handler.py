import json
import logging
import os
import boto3
from typing import Dict, Any
from datetime import datetime
from services.dynamodb_client import DynamoDBClient
from services.namespace_migrator import NamespaceMigrator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def migrate_namespaces_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for initiating namespace migration.
    
    Args:
        event: Lambda event containing migration parameters
        context: Lambda context object
        
    Returns:
        Dict: Migration task response
    """
    try:
        # Parse request parameters
        query_params = event.get('queryStringParameters') or {}
        headers = event.get('headers', {})
        
        # Validate API key
        api_key = headers.get('x-api-key')
        expected_api_key = os.environ.get('API_KEY')
        
        if not api_key or api_key != expected_api_key:
            return create_response(401, {'error': 'Unauthorized'})
        
        # Get migration parameters
        batch_size = int(query_params.get('batch_size', '100'))
        rate_limit = int(query_params.get('rate_limit', '10'))
        resume_token = query_params.get('resume_token')
        dry_run = query_params.get('dry_run', 'false').lower() == 'true'
        source_namespace = query_params.get('source_namespace', '')  # Empty string for default
        
        # Optional namespace mapping or dynamic mode
        use_dynamic_namespaces = query_params.get('use_dynamic_namespaces', 'false').lower() == 'true'
        namespace_mapping = query_params.get('namespace_mapping')
        
        if use_dynamic_namespaces:
            # Use dynamic namespace creation - pass special flag
            namespace_mapping = {'use_dynamic': True}
            logger.info("Using dynamic namespace creation based on record_type values")
        elif namespace_mapping:
            try:
                namespace_mapping = json.loads(namespace_mapping)
            except json.JSONDecodeError:
                return create_response(400, {'error': 'Invalid namespace_mapping JSON'})
        else:
            # Default to dynamic mode if no mapping provided
            namespace_mapping = {'use_dynamic': True}
            logger.info("No namespace mapping provided, using dynamic namespace creation")
        
        logger.info(f"Starting namespace migration (dry_run={dry_run}, batch_size={batch_size}, rate_limit={rate_limit})")
        
        # Initialize DynamoDB client
        dynamodb_client = DynamoDBClient()
        
        # Create migration task
        task_id = dynamodb_client.create_task(
            start_date=datetime.utcnow().strftime('%Y-%m-%d'),
            end_date=datetime.utcnow().strftime('%Y-%m-%d'),
            task_type='namespace_migration'
        )
        
        # Add migration-specific fields to task
        dynamodb_client.update_task_metadata(task_id, {
            'migration_type': 'namespace_migration',
            'source_namespace': source_namespace,
            'namespace_mapping': namespace_mapping,
            'batch_size': batch_size,
            'rate_limit': rate_limit,
            'dry_run': dry_run,
            'resume_token': resume_token or '',
            'migration_progress': {
                'phase': 'INITIALIZING',
                'vectors_processed': 0,
                'vectors_failed': 0,
                'current_batch': 0,
                'total_batches': 0
            }
        })
        
        # Trigger Step Functions state machine for large-scale processing
        step_functions = boto3.client('stepfunctions')
        state_machine_arn = os.environ.get('MIGRATION_STATE_MACHINE_ARN')
        
        if state_machine_arn:
            # Use Step Functions for large migrations
            execution_input = {
                'task_id': task_id,
                'source_namespace': source_namespace,
                'namespace_mapping': namespace_mapping,
                'batch_size': batch_size,
                'rate_limit': rate_limit,
                'dry_run': dry_run,
                'resume_token': resume_token
            }
            
            step_functions.start_execution(
                stateMachineArn=state_machine_arn,
                name=f"migration-{task_id}",
                input=json.dumps(execution_input)
            )
            
            logger.info(f"Started Step Functions execution for migration task {task_id}")
        else:
            # Fallback to async Lambda for smaller migrations
            lambda_client = boto3.client('lambda')
            
            async_payload = {
                'task_id': task_id,
                'source_namespace': source_namespace,
                'namespace_mapping': namespace_mapping,
                'batch_size': batch_size,
                'rate_limit': rate_limit,
                'dry_run': dry_run,
                'resume_token': resume_token
            }
            
            lambda_client.invoke(
                FunctionName=os.environ.get('MIGRATION_ASYNC_FUNCTION_NAME', 'vectordb-migration-async-dev'),
                InvocationType='Event',
                Payload=json.dumps(async_payload)
            )
            
            logger.info(f"Started async Lambda execution for migration task {task_id}")
        
        # Return task information
        response_data = {
            'message': 'Migration started',
            'task_id': task_id,
            'source_namespace': source_namespace,
            'namespace_mode': 'dynamic' if namespace_mapping.get('use_dynamic') else 'mapped',
            'batch_size': batch_size,
            'rate_limit': rate_limit,
            'dry_run': dry_run,
            'status': 'PENDING',
            'status_url': f"/status?task_id={task_id}"
        }
        
        # Only include target_namespaces if using mapped mode
        if not namespace_mapping.get('use_dynamic'):
            response_data['target_namespaces'] = list(set(namespace_mapping.values()))
        
        if resume_token:
            response_data['resumed_from'] = resume_token
        
        return create_response(202, response_data)
        
    except Exception as e:
        logger.error(f"Error starting migration: {e}")
        return create_response(500, {'error': 'Failed to start migration'})


def async_migration_handler(event: Dict[str, Any], context: Any) -> None:
    """
    Async handler for processing namespace migration.
    
    Args:
        event: Lambda event containing migration parameters
        context: Lambda context object
    """
    task_id = None
    try:
        # Extract parameters
        task_id = event.get('task_id')
        source_namespace = event.get('source_namespace', '')
        namespace_mapping = event.get('namespace_mapping', {})
        batch_size = event.get('batch_size', 100)
        rate_limit = event.get('rate_limit', 10)
        dry_run = event.get('dry_run', False)
        resume_token = event.get('resume_token')
        
        logger.info(f"Starting async migration for task {task_id}")
        
        # Initialize clients
        dynamodb_client = DynamoDBClient()
        migrator = NamespaceMigrator(dry_run=dry_run)
        
        # Update environment variables for migration
        os.environ['MIGRATION_BATCH_SIZE'] = str(batch_size)
        os.environ['MIGRATION_RATE_LIMIT'] = str(rate_limit)
        
        try:
            # Update task status to IN_PROGRESS
            dynamodb_client.update_task_status(task_id, 'IN_PROGRESS')
            dynamodb_client.update_task_metadata(task_id, {
                'migration_progress.phase': 'INVENTORY'
            })
            
            # Phase 1: Inventory
            logger.info("Phase 1: Starting inventory of record types")
            inventory = migrator.inventory_record_types(
                namespace=source_namespace,
                sample_size=5000
            )
            
            total_vectors = inventory.get('total_vectors', 0)
            record_types = inventory.get('record_types', {})
            
            dynamodb_client.update_task_metadata(task_id, {
                'inventory': inventory,
                'total_records': total_vectors,
                'migration_progress.phase': 'MIGRATION',
                'migration_progress.total_batches': (total_vectors + batch_size - 1) // batch_size
            })
            
            logger.info(f"Inventory complete: {total_vectors} vectors, {len(record_types)} record types")
            
            # Phase 2: Migration
            logger.info("Phase 2: Starting migration")
            
            # Parse resume token if provided
            last_processed_id = None
            if resume_token:
                resume_data = migrator.parse_resume_token(resume_token)
                last_processed_id = resume_data.get('last_id')
                logger.info(f"Resuming from vector ID: {last_processed_id}")
            
            # Process vectors in batches
            vectors_processed = 0
            vectors_failed = 0
            current_batch = 0
            rollback_data = []
            next_page_token = None
            skip_until_resume = bool(last_processed_id)
            
            while vectors_processed < total_vectors:
                # List vectors with pagination using list_paginated
                list_kwargs = {
                    'namespace': source_namespace,
                    'limit': batch_size
                }
                if next_page_token:
                    list_kwargs['pagination_token'] = next_page_token
                    
                list_response = migrator.index.list_paginated(**list_kwargs)
                
                # Extract vector list and convert to IDs
                vector_list = list_response.vectors if hasattr(list_response, 'vectors') else []
                if not vector_list:
                    break
                
                # Extract actual ID strings from vector objects
                vector_ids = [v.id if hasattr(v, 'id') else str(v) for v in vector_list]
                
                # Skip vectors until we reach resume point
                if skip_until_resume:
                    if last_processed_id in vector_ids:
                        # Found resume point, process from next vector
                        idx = vector_ids.index(last_processed_id)
                        vector_ids = vector_ids[idx + 1:]
                        skip_until_resume = False
                    else:
                        # Haven't reached resume point yet, skip this batch
                        next_page_token = getattr(list_response, 'pagination_token', None)
                        continue
                
                # Migrate batch
                success, failed, batch_rollback = migrator.migrate_batch(
                    vector_ids=vector_ids,
                    source_namespace=source_namespace,
                    target_namespace_map=namespace_mapping,
                    rollback_data=rollback_data
                )
                
                vectors_processed += success
                vectors_failed += failed
                current_batch += 1
                
                # Update progress
                dynamodb_client.update_task_metadata(task_id, {
                    'migration_progress.vectors_processed': vectors_processed,
                    'migration_progress.vectors_failed': vectors_failed,
                    'migration_progress.current_batch': current_batch,
                    'processed_records': vectors_processed,
                    'failed_records': vectors_failed
                })
                
                # Generate resume token
                if vector_ids:
                    resume_token = migrator.generate_resume_token(
                        vector_ids[-1],
                        source_namespace
                    )
                    dynamodb_client.update_task_metadata(task_id, {
                        'resume_token': resume_token
                    })
                
                # Check for Lambda timeout (leave 30 seconds for cleanup)
                if context.get_remaining_time_in_millis() < 30000:
                    logger.warning("Lambda timeout approaching, saving progress")
                    dynamodb_client.update_task_metadata(task_id, {
                        'migration_progress.phase': 'PAUSED',
                        'pause_reason': 'Lambda timeout'
                    })
                    return
                
                next_page_token = getattr(list_response, 'pagination_token', None)
                if not next_page_token:
                    break
                
                # Log progress
                if current_batch % 10 == 0:
                    logger.info(f"Progress: {vectors_processed}/{total_vectors} vectors migrated")
            
            # Phase 3: Verification
            logger.info("Phase 3: Starting verification")
            dynamodb_client.update_task_metadata(task_id, {
                'migration_progress.phase': 'VERIFICATION'
            })
            
            target_namespaces = list(set(namespace_mapping.values()))
            verification_result = migrator.verify_migration(
                source_namespace=source_namespace,
                target_namespaces=target_namespaces,
                sample_size=100
            )
            
            dynamodb_client.update_task_metadata(task_id, {
                'verification_result': verification_result,
                'rollback_data_size': len(rollback_data)
            })
            
            # Update final task status
            final_status = 'COMPLETED' if vectors_failed == 0 else 'COMPLETED_WITH_ERRORS'
            dynamodb_client.update_task_status(
                task_id,
                final_status,
                processed_records=vectors_processed,
                failed_records=vectors_failed
            )
            
            logger.info(f"Migration completed: {vectors_processed} processed, {vectors_failed} failed")
            
        except Exception as e:
            logger.error(f"Error during migration: {e}")
            dynamodb_client.set_task_error(task_id, str(e))
            raise
            
    except Exception as e:
        logger.error(f"Migration handler error: {e}")
        if task_id:
            try:
                dynamodb_client = DynamoDBClient()
                dynamodb_client.set_task_error(task_id, str(e))
            except:
                pass


def rollback_migration_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for rolling back a migration.
    
    Args:
        event: Lambda event containing task_id
        context: Lambda context object
        
    Returns:
        Dict: Rollback result
    """
    try:
        # Parse request
        query_params = event.get('queryStringParameters') or {}
        headers = event.get('headers', {})
        
        # Validate API key
        api_key = headers.get('x-api-key')
        expected_api_key = os.environ.get('API_KEY')
        
        if not api_key or api_key != expected_api_key:
            return create_response(401, {'error': 'Unauthorized'})
        
        task_id = query_params.get('task_id')
        if not task_id:
            return create_response(400, {'error': 'task_id parameter is required'})
        
        # Get task data from DynamoDB
        dynamodb_client = DynamoDBClient()
        task = dynamodb_client.get_task(task_id)
        
        if not task:
            return create_response(404, {'error': 'Task not found'})
        
        if task.get('migration_type') != 'namespace_migration':
            return create_response(400, {'error': 'Task is not a migration task'})
        
        # Note: Actual rollback would require storing rollback data
        # This is a placeholder for the rollback logic
        return create_response(200, {
            'message': 'Rollback functionality requires rollback data storage',
            'task_id': task_id,
            'status': 'ROLLBACK_NOT_AVAILABLE'
        })
        
    except Exception as e:
        logger.error(f"Error in rollback handler: {e}")
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