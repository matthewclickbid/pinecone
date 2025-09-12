"""
FastAPI application for VectorDB processing on EC2.
Handles large-scale CSV processing without Lambda limitations.
"""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

from app.api import routes
from app.api import monitoring
from app.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown events"""
    # Startup
    logger.info("Starting VectorDB Processing API with Background Tasks...")
    logger.info(f"Environment: {settings.ENVIRONMENT}")
    logger.info(f"API Version: 2.0.0")
    logger.info(f"Redis URL: {settings.REDIS_HOST}:{settings.REDIS_PORT}")
    logger.info(f"Max Workers: {settings.MAX_WORKERS}")
    
    # Initialize connections here if needed
    # e.g., Redis, DynamoDB connection pools
    
    yield
    
    # Shutdown
    logger.info("Shutting down VectorDB Processing API...")
    # Clean up connections here


# Create FastAPI application
app = FastAPI(
    title="VectorDB Processing API",
    description="EC2-based API for processing large CSV files and generating vector embeddings with Celery background tasks",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/api/v1/docs" if settings.ENVIRONMENT != "production" else None,
    redoc_url="/api/v1/redoc" if settings.ENVIRONMENT != "production" else None,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle all uncaught exceptions"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc) if settings.ENVIRONMENT == "development" else "An error occurred"
        }
    )


# Include API routes
app.include_router(routes.router, prefix="/api/v1")
app.include_router(monitoring.router, prefix="/api/v1")


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint - API information"""
    return {
        "name": "VectorDB Processing API",
        "version": "2.0.0",
        "status": "running",
        "environment": settings.ENVIRONMENT,
        "features": [
            "Celery background processing",
            "Real-time progress tracking",
            "Health monitoring",
            "Task queue management"
        ],
        "docs": "/api/v1/docs" if settings.ENVIRONMENT != "production" else None
    }


# Health check endpoint
@app.get("/health")
async def health_check():
    """Basic health check endpoint for load balancer"""
    return {
        "status": "healthy",
        "version": "2.0.0",
        "environment": settings.ENVIRONMENT,
        "timestamp": "2025-01-01T00:00:00Z"
    }


if __name__ == "__main__":
    # Run with uvicorn for development
    uvicorn.run(
        "app.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.ENVIRONMENT == "development",
        log_level="info"
    )