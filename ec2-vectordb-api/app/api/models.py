"""
Pydantic models for API request/response validation.
"""

from typing import Optional, Dict, Any, Literal
from datetime import datetime
from pydantic import BaseModel, Field, validator
from uuid import UUID


class ProcessRequest(BaseModel):
    """Request model for processing data"""
    
    data_source: Literal["s3_csv", "metabase"] = Field(
        ...,
        description="Data source type"
    )
    
    # S3 CSV specific fields
    s3_key: Optional[str] = Field(
        None,
        description="S3 key for CSV file (required if data_source is s3_csv)"
    )
    
    # Question ID - used for Metabase and for vector ID formatting
    question_id: Optional[int] = Field(
        None,
        description="Question ID - required for Metabase, optional for CSV (used in vector ID as question_id.row_id)"
    )
    
    start_date: Optional[str] = Field(
        None,
        description="Start date for data filtering (YYYY-MM-DD format)"
    )
    
    end_date: Optional[str] = Field(
        None,
        description="End date for data filtering (YYYY-MM-DD format)"
    )
    
    date_column: str = Field(
        default="created_at",
        description="Date column name for filtering"
    )
    
    batch_size: int = Field(
        default=1000,
        ge=100,
        le=10000,
        description="Batch size for processing"
    )
    
    chunk_size: int = Field(
        default=1000,
        ge=100,
        le=100000,
        description="Chunk size for CSV processing"
    )
    
    namespace: str = Field(
        default="v2",
        description="Pinecone namespace for vectors"
    )
    
    priority: Literal["low", "normal", "high"] = Field(
        default="normal",
        description="Processing priority"
    )
    
    @validator("s3_key")
    def validate_s3_key(cls, v, values):
        """Validate s3_key is provided when data_source is s3_csv"""
        if values.get("data_source") == "s3_csv" and not v:
            raise ValueError("s3_key is required when data_source is s3_csv")
        return v
    
    @validator("question_id")
    def validate_question_id(cls, v, values):
        """Validate question_id is provided when data_source is metabase"""
        if values.get("data_source") == "metabase" and not v:
            raise ValueError("question_id is required when data_source is metabase")
        return v
    
    @validator("start_date", "end_date")
    def validate_date_format(cls, v):
        """Validate date format"""
        if v:
            try:
                datetime.strptime(v, "%Y-%m-%d")
            except ValueError:
                raise ValueError("Date must be in YYYY-MM-DD format")
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "data_source": "s3_csv",
                "s3_key": "data/large_dataset.csv",
                "batch_size": 1000,
                "namespace": "v2",
                "priority": "normal"
            }
        }


class ProcessResponse(BaseModel):
    """Response model for process request"""
    
    task_id: str = Field(..., description="Unique task identifier")
    status: Literal["pending", "queued", "processing", "processing_chunks", "partially_completed", "completed", "failed", "cancelled"] = Field(
        ...,
        description="Current task status"
    )
    message: str = Field(..., description="Status message")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        schema_extra = {
            "example": {
                "task_id": "123e4567-e89b-12d3-a456-426614174000",
                "status": "queued",
                "message": "Task queued for processing",
                "created_at": "2025-01-01T00:00:00Z"
            }
        }


class TaskProgress(BaseModel):
    """Progress information for a task"""
    
    total_records: int = Field(0, description="Total number of records to process")
    processed_records: int = Field(0, description="Number of records processed")
    failed_records: int = Field(0, description="Number of failed records")
    chunks_total: int = Field(0, description="Total number of chunks")
    chunks_completed: int = Field(0, description="Number of completed chunks")
    chunks_processing: int = Field(0, description="Number of chunks currently being processed")
    chunks_failed: int = Field(0, description="Number of failed chunks")
    current_chunk: Optional[int] = Field(None, description="Currently processing chunk")
    percentage: float = Field(0.0, description="Completion percentage")
    
    @validator("percentage", always=True)
    def calculate_percentage(cls, v, values):
        """Calculate completion percentage"""
        total = values.get("total_records", 0)
        processed = values.get("processed_records", 0)
        if total > 0:
            return round((processed / total) * 100, 2)
        return 0.0


class TaskStatus(BaseModel):
    """Task status response model"""
    
    task_id: str = Field(..., description="Unique task identifier")
    status: Literal["pending", "queued", "processing", "processing_chunks", "partially_completed", "completed", "failed", "cancelled"] = Field(
        ...,
        description="Current task status"
    )
    progress: TaskProgress = Field(default_factory=TaskProgress)
    started_at: Optional[datetime] = Field(None, description="Task start time")
    completed_at: Optional[datetime] = Field(None, description="Task completion time")
    error: Optional[str] = Field(None, description="Error message if failed")
    result: Optional[Dict[str, Any]] = Field(None, description="Task result data")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    class Config:
        schema_extra = {
            "example": {
                "task_id": "123e4567-e89b-12d3-a456-426614174000",
                "status": "processing",
                "progress": {
                    "total_records": 100000,
                    "processed_records": 50000,
                    "failed_records": 10,
                    "chunks_total": 100,
                    "chunks_completed": 50,
                    "current_chunk": 51,
                    "percentage": 50.0
                },
                "started_at": "2025-01-01T00:00:00Z",
                "completed_at": None,
                "error": None
            }
        }


class HealthResponse(BaseModel):
    """Health check response model"""
    
    status: Literal["healthy", "degraded", "unhealthy"] = Field(
        ...,
        description="Service health status"
    )
    version: str = Field(..., description="API version")
    environment: str = Field(..., description="Environment name")
    workers: Optional[Dict[str, int]] = Field(
        None,
        description="Worker status information"
    )
    services: Dict[str, str] = Field(
        default_factory=dict,
        description="External service status"
    )
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        schema_extra = {
            "example": {
                "status": "healthy",
                "version": "1.0.0",
                "environment": "production",
                "workers": {
                    "active": 4,
                    "available": 4,
                    "queued_tasks": 10
                },
                "services": {
                    "redis": "connected",
                    "dynamodb": "connected",
                    "s3": "connected"
                },
                "timestamp": "2025-01-01T00:00:00Z"
            }
        }


class ErrorResponse(BaseModel):
    """Error response model"""
    
    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
    request_id: Optional[str] = Field(None, description="Request ID for tracking")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        schema_extra = {
            "example": {
                "error": "ValidationError",
                "message": "Invalid request parameters",
                "details": {
                    "field": "s3_key",
                    "reason": "s3_key is required when data_source is s3_csv"
                },
                "request_id": "req_123456",
                "timestamp": "2025-01-01T00:00:00Z"
            }
        }