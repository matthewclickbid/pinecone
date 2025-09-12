"""
Configuration management for the VectorDB Processing API.
Uses pydantic settings for environment variable management and validation.
"""

import os
from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Environment
    ENVIRONMENT: str = Field(default="development", description="Environment (development, staging, production)")
    
    # API Configuration
    API_KEY: str = Field(..., description="API key for authentication")
    API_HOST: str = Field(default="0.0.0.0", description="API host")
    API_PORT: int = Field(default=8000, description="API port")
    API_WORKERS: int = Field(default=4, description="Number of API workers")
    ALLOWED_ORIGINS: List[str] = Field(default=["*"], description="Allowed CORS origins")
    
    # External Services
    METABASE_URL: str = Field(default="https://cbmetabase.com", description="Metabase URL")
    METABASE_API_KEY: str = Field(..., description="Metabase API key")
    METABASE_QUESTION_ID: str = Field(default="232", description="Default Metabase question ID")
    
    OPENAI_API_KEY: str = Field(..., description="OpenAI API key")
    OPENAI_MODEL: str = Field(default="text-embedding-3-small", description="OpenAI embedding model")
    OPENAI_RATE_LIMIT: int = Field(default=20, description="OpenAI requests per second")
    
    PINECONE_API_KEY: str = Field(..., description="Pinecone API key")
    PINECONE_INDEX_NAME: str = Field(default="clickbidtest", description="Pinecone index name")
    PINECONE_NAMESPACE: str = Field(default="v2", description="Pinecone namespace")
    PINECONE_BATCH_SIZE: int = Field(default=100, description="Pinecone batch upload size")
    
    # AWS Configuration
    AWS_REGION: str = Field(default="us-east-1", description="AWS region")
    DYNAMODB_TABLE_NAME: str = Field(default="vectordb-tasks-ec2", description="DynamoDB table name")
    S3_BUCKET_NAME: str = Field(default="pineconeuploads", description="S3 bucket name")
    
    # Local File Configuration
    USE_LOCAL_FILES: bool = Field(default=False, description="Use local filesystem instead of S3")
    LOCAL_CSV_FOLDER: str = Field(default="./test_data/csv_files", description="Local folder for CSV files")
    
    # Redis Configuration (for future Celery integration)
    REDIS_HOST: str = Field(default="localhost", description="Redis host")
    REDIS_PORT: int = Field(default=6379, description="Redis port")
    REDIS_DB: int = Field(default=0, description="Redis database")
    REDIS_PASSWORD: Optional[str] = Field(default=None, description="Redis password")
    
    # Processing Configuration
    CHUNK_SIZE: int = Field(default=1000, description="CSV chunk size for processing")
    BATCH_SIZE: int = Field(default=100, description="Batch size for vector operations")
    MAX_WORKERS: int = Field(default=8, description="Maximum parallel workers")
    TASK_TIMEOUT: int = Field(default=3600, description="Task timeout in seconds")
    
    # Logging
    LOG_LEVEL: str = Field(default="INFO", description="Logging level")
    LOG_FORMAT: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format"
    )
    
    class Config:
        """Pydantic configuration"""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
        extra = "allow"  # Allow extra fields from .env
        
    @property
    def redis_url(self) -> str:
        """Generate Redis connection URL"""
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development environment"""
        return self.ENVIRONMENT == "development"
    
    @property
    def is_production(self) -> bool:
        """Check if running in production environment"""
        return self.ENVIRONMENT == "production"
    
    @property
    def is_local_mode(self) -> bool:
        """Check if using local file system instead of S3"""
        return self.USE_LOCAL_FILES
    
    def get_dynamodb_table_name(self) -> str:
        """Get environment-specific DynamoDB table name"""
        if self.ENVIRONMENT != "production":
            return f"{self.DYNAMODB_TABLE_NAME}-{self.ENVIRONMENT}"
        return self.DYNAMODB_TABLE_NAME
    
    def get_csv_file_path(self, file_key: str) -> str:
        """Get the full path to a CSV file (local or S3 key)"""
        if self.USE_LOCAL_FILES:
            # Convert S3-style path to local path
            local_path = os.path.join(self.LOCAL_CSV_FOLDER, file_key)
            return os.path.abspath(local_path)
        return file_key  # Return S3 key as-is


@lru_cache()
def get_settings() -> Settings:
    """
    Create and cache settings instance.
    Use lru_cache to ensure we only create one instance.
    """
    return Settings()


# Create a global settings instance
settings = get_settings()