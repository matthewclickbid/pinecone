"""
API routes for VectorDB processing with background task integration.
"""

import logging
import uuid
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse

from app.api.models import (
    ProcessRequest,
    ProcessResponse,
    TaskStatus,
    HealthResponse,
    ErrorResponse,
    TaskProgress
)
from app.api.auth import verify_api_key
from app.config import settings
from app.services.dynamodb_client import DynamoDBClient
from app.utils.progress_tracker import progress_tracker
from app.tasks.data_processing import process_metabase_data, process_s3_csv_data

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()


@router.post(
    "/process",
    response_model=ProcessResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        400: {"model": ErrorResponse, "description": "Bad Request"},
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        500: {"model": ErrorResponse, "description": "Internal Server Error"}
    },
    dependencies=[Depends(verify_api_key)]
)
async def process_data(request: ProcessRequest):
    """
    Submit a data processing task for background execution.
    
    This endpoint accepts a request to process data from either S3 CSV or Metabase,
    generates vector embeddings, and stores them in Pinecone.
    
    The processing is done asynchronously using Celery, and this endpoint returns
    immediately with a task ID that can be used to track progress.
    """
    try:
        # Initialize DynamoDB client
        dynamodb = DynamoDBClient()
        
        # Log request
        logger.info(f"Processing request received: data_source={request.data_source}")
        
        # Validate request parameters
        if request.data_source == "metabase" and not request.start_date:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="start_date is required for Metabase data processing"
            )
        
        if request.data_source == "s3_csv" and not request.s3_key:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="s3_key is required for S3 CSV data processing"
            )
        
        # Create task parameters
        task_parameters = {
            "data_source": request.data_source,
            "start_date": request.start_date,
            "end_date": request.end_date,
            "s3_key": request.s3_key,
            "namespace": request.namespace or settings.PINECONE_NAMESPACE,
            "question_id": request.question_id or settings.METABASE_QUESTION_ID,
            "batch_size": request.batch_size or settings.BATCH_SIZE,
            "chunk_size": request.chunk_size or settings.CHUNK_SIZE
        }
        
        # Create background task in DynamoDB
        task_type = f"{request.data_source}_processing"
        task_id = dynamodb.create_background_task(
            task_type=task_type,
            parameters=task_parameters,
            priority="normal"
        )
        
        # Dispatch appropriate Celery task
        if request.data_source == "metabase":
            celery_task = process_metabase_data.delay(
                task_id=task_id,
                start_date=request.start_date,
                end_date=request.end_date,
                question_id=request.question_id,
                namespace=request.namespace,
                batch_size=request.batch_size
            )
        elif request.data_source == "s3_csv":
            celery_task = process_s3_csv_data.delay(
                task_id=task_id,
                s3_key=request.s3_key,
                question_id=str(request.question_id) if request.question_id else None,
                namespace=request.namespace,
                chunk_size=request.chunk_size
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported data source: {request.data_source}"
            )
        
        # Update task with Celery task ID
        dynamodb.update_task_status(
            task_id,
            status="PENDING",
            celery_task_id=celery_task.id,
            dispatched_at=datetime.utcnow().isoformat()
        )
        
        # Initialize progress tracking
        progress_tracker.update_progress(
            task_id=task_id,
            current=0,
            total=0,
            status="PENDING",
            message=f"Task queued for {request.data_source} processing",
            persist_to_db=False  # Already updated above
        )
        
        # Log successful task creation
        logger.info(f"Task {task_id} created and dispatched to Celery (task_id: {celery_task.id})")
        
        # Return response
        return ProcessResponse(
            task_id=task_id,
            status="pending",
            message=f"Task queued for {request.data_source} processing",
            created_at=datetime.utcnow()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating task: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create task: {str(e)}"
        )


@router.get(
    "/status/{task_id}",
    response_model=TaskStatus,
    responses={
        404: {"model": ErrorResponse, "description": "Task not found"},
        401: {"model": ErrorResponse, "description": "Unauthorized"}
    },
    dependencies=[Depends(verify_api_key)]
)
async def get_task_status(task_id: str):
    """
    Get the status of a processing task.
    
    Returns detailed information about the task including real-time progress,
    status, and any errors that may have occurred.
    """
    try:
        # Initialize DynamoDB client
        dynamodb = DynamoDBClient()
        
        # Get task from DynamoDB
        task = dynamodb.get_task(task_id)
        
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task {task_id} not found"
            )
        
        # Get real-time progress information
        progress_data = progress_tracker.get_progress(task_id)
        
        # Create progress object - prioritize progress_data, fall back to task data
        # Handle both naming conventions: chunks_total/total_chunks and chunks_completed/completed_chunks
        # Progress data from Redis uses 'total' and 'current', task data uses 'total_records' and 'processed_records'
        total_records = 0
        processed_records = 0
        
        if progress_data:
            total_records = progress_data.get('total', 0) or task.get('total_records', 0)
            processed_records = progress_data.get('current', 0) or task.get('processed_records', 0)
        else:
            total_records = task.get('total_records', 0)
            processed_records = task.get('processed_records', 0)
        
        progress = TaskProgress(
            total_records=total_records,
            processed_records=processed_records,
            failed_records=task.get('failed_records', 0),
            chunks_total=task.get('chunks_total', task.get('total_chunks', 0)),
            chunks_completed=task.get('chunks_completed', task.get('completed_chunks', 0)),
            chunks_processing=task.get('chunks_processing', 0),
            chunks_failed=task.get('chunks_failed', 0),
            current_chunk=task.get('current_chunk'),
            percentage=progress_data.get('progress_percentage', 0.0) if progress_data else task.get('progress_percentage', 0.0)
        )
        
        # Parse timestamps
        started_at = None
        completed_at = None
        
        if task.get('started_at'):
            try:
                started_at = datetime.fromisoformat(task['started_at'].replace('Z', '+00:00'))
            except:
                pass
        
        if task.get('completed_at'):
            try:
                completed_at = datetime.fromisoformat(task['completed_at'].replace('Z', '+00:00'))
            except:
                pass
        
        # Return task status
        return TaskStatus(
            task_id=task_id,
            status=task["status"].lower(),  # Normalize status to lowercase
            progress=progress,
            started_at=started_at,
            completed_at=completed_at,
            error=task.get("error_message"),
            metadata={
                "task_type": task.get("task_type"),
                "parameters": task.get("parameters", {}),
                "celery_task_id": task.get("celery_task_id"),
                "created_at": task.get("created_at"),
                "estimated_completion_time": progress_data.get('estimated_completion_time') if progress_data else None,
                "namespace": task.get("parameters", {}).get("namespace") if task.get("parameters") else None
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting task status: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get task status: {str(e)}"
        )


@router.delete(
    "/task/{task_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        404: {"model": ErrorResponse, "description": "Task not found"},
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        409: {"model": ErrorResponse, "description": "Task cannot be cancelled"}
    },
    dependencies=[Depends(verify_api_key)]
)
async def cancel_task(task_id: str):
    """
    Cancel a processing task.
    
    This will attempt to cancel a Celery task and update the task status in DynamoDB.
    Tasks that are already completed or failed cannot be cancelled.
    """
    try:
        # Initialize DynamoDB client
        dynamodb = DynamoDBClient()
        
        # Get task from DynamoDB
        task = dynamodb.get_task(task_id)
        
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task {task_id} not found"
            )
        
        # Check if task can be cancelled
        if task["status"] in ["COMPLETED", "FAILED", "CANCELLED"]:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Task {task_id} is already {task['status'].lower()} and cannot be cancelled"
            )
        
        # Cancel Celery task if it exists
        celery_task_id = task.get("celery_task_id")
        if celery_task_id:
            try:
                from app.celery_config import celery_app
                celery_app.control.revoke(celery_task_id, terminate=True)
                logger.info(f"Cancelled Celery task {celery_task_id} for task {task_id}")
            except Exception as e:
                logger.warning(f"Failed to cancel Celery task {celery_task_id}: {e}")
        
        # Update task status in DynamoDB
        dynamodb.cancel_task(task_id, reason="Cancelled by user request")
        
        # Clean up progress tracking
        progress_tracker.cleanup_completed_progress(task_id)
        
        logger.info(f"Task {task_id} cancelled successfully")
        return None
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling task: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel task: {str(e)}"
        )


@router.get(
    "/health",
    response_model=HealthResponse,
    responses={
        503: {"model": ErrorResponse, "description": "Service Unavailable"}
    }
)
async def health_check():
    """
    Basic health check endpoint.
    
    Returns the health status of the API and its dependencies.
    This endpoint does not require authentication.
    """
    try:
        # Initialize clients for health checks
        dynamodb = DynamoDBClient()
        
        # Check service health
        health_status = "healthy"
        services_status = {}
        
        # Check Redis connection
        try:
            import redis
            redis_client = redis.from_url(settings.redis_url)
            redis_client.ping()
            services_status["redis"] = "healthy"
        except Exception as e:
            logger.warning(f"Redis health check failed: {e}")
            services_status["redis"] = "unhealthy"
            health_status = "degraded"
        
        # Check DynamoDB connection
        try:
            # Simple table describe to test connectivity
            dynamodb.table.meta.client.describe_table(TableName=dynamodb.table_name)
            services_status["dynamodb"] = "healthy"
        except Exception as e:
            logger.warning(f"DynamoDB health check failed: {e}")
            services_status["dynamodb"] = "unhealthy"
            health_status = "degraded"
        
        # Check API status
        services_status["api"] = "healthy"
        
        # Get worker information
        workers_info = {"active": 0, "available": settings.MAX_WORKERS, "queued_tasks": 0}
        
        try:
            from app.celery_config import celery_app
            inspect = celery_app.control.inspect()
            
            # Get active tasks
            active_tasks = inspect.active()
            if active_tasks:
                workers_info["active"] = sum(len(tasks) for tasks in active_tasks.values())
            
            # Get queue lengths
            import redis
            redis_client = redis.from_url(settings.redis_url)
            queue_names = ['data_processing', 'chunk_processing', 'monitoring']
            workers_info["queued_tasks"] = sum(
                redis_client.llen(f"celery:{name}") for name in queue_names
            )
        except Exception as e:
            logger.warning(f"Worker status check failed: {e}")
        
        # Set overall status
        if any(status == "unhealthy" for status in services_status.values()):
            health_status = "degraded"
        
        return HealthResponse(
            status=health_status,
            version="2.0.0",
            environment=settings.ENVIRONMENT,
            workers=workers_info,
            services=services_status
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Service health check failed: {str(e)}"
        )


@router.get(
    "/tasks",
    response_model=list[TaskStatus],
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"}
    },
    dependencies=[Depends(verify_api_key)]
)
async def list_tasks(
    status_filter: Optional[str] = None,
    task_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    """
    List tasks with optional filtering and pagination.
    
    This endpoint returns a list of tasks from DynamoDB, optionally filtered by status
    or task type. Pagination is supported through limit and offset parameters.
    """
    try:
        # Initialize DynamoDB client
        dynamodb = DynamoDBClient()
        
        # Get tasks based on filters
        if status_filter:
            tasks = dynamodb.get_tasks_by_status(status_filter.upper(), task_type, limit)
        else:
            # Get active tasks by default (can be extended to get all)
            tasks = dynamodb.get_active_tasks(limit)
            if task_type:
                tasks = [t for t in tasks if t.get('task_type') == task_type]
        
        # Apply offset (simple slicing for now - in production might want to use DynamoDB pagination)
        paginated_tasks = tasks[offset:offset + limit] if offset > 0 else tasks
        
        # Convert to response model
        response = []
        for task in paginated_tasks:
            # Get progress data
            progress_data = progress_tracker.get_progress(task['task_id'])
            
            # Create progress object - prioritize progress_data, fall back to task data
            progress = TaskProgress(
                total_records=progress_data.get('total', 0) if progress_data else task.get('total_records', 0),
                processed_records=progress_data.get('current', 0) if progress_data else task.get('processed_records', 0),
                failed_records=task.get('failed_records', 0),
                percentage=progress_data.get('progress_percentage', 0.0) if progress_data else task.get('progress_percentage', 0.0)
            )
            
            # Parse timestamps
            started_at = None
            completed_at = None
            
            if task.get('started_at'):
                try:
                    started_at = datetime.fromisoformat(task['started_at'].replace('Z', '+00:00'))
                except:
                    pass
            
            if task.get('completed_at'):
                try:
                    completed_at = datetime.fromisoformat(task['completed_at'].replace('Z', '+00:00'))
                except:
                    pass
            
            response.append(TaskStatus(
                task_id=task["task_id"],
                status=task["status"].lower(),
                progress=progress,
                started_at=started_at,
                completed_at=completed_at,
                error=task.get("error_message"),
                metadata={
                    "task_type": task.get("task_type"),
                    "parameters": task.get("parameters", {}),
                    "created_at": task.get("created_at"),
                    "celery_task_id": task.get("celery_task_id")
                }
            ))
        
        return response
        
    except Exception as e:
        logger.error(f"Error listing tasks: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list tasks: {str(e)}"
        )