"""
Authentication middleware for API key verification.
"""

import logging
from typing import Optional
from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader
from app.config import settings

# Configure logging
logger = logging.getLogger(__name__)

# API Key header configuration
api_key_header = APIKeyHeader(
    name="X-API-Key",
    auto_error=False,
    description="API key for authentication"
)


async def verify_api_key(api_key: Optional[str] = Security(api_key_header)) -> str:
    """
    Verify API key from request header.
    
    Args:
        api_key: API key from X-API-Key header
        
    Returns:
        str: The validated API key
        
    Raises:
        HTTPException: If API key is missing or invalid
    """
    if not api_key:
        logger.warning("Request received without API key")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key is missing",
            headers={"WWW-Authenticate": "ApiKey"}
        )
    
    # Verify API key matches configured key
    if api_key != settings.API_KEY:
        logger.warning(f"Invalid API key attempted: {api_key[:8]}...")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "ApiKey"}
        )
    
    return api_key


async def get_optional_api_key(api_key: Optional[str] = Security(api_key_header)) -> Optional[str]:
    """
    Get API key if provided, but don't require it.
    Useful for endpoints that have different behavior for authenticated vs public access.
    
    Args:
        api_key: API key from X-API-Key header
        
    Returns:
        Optional[str]: The API key if valid, None otherwise
    """
    if not api_key:
        return None
    
    # Verify API key if provided
    if api_key == settings.API_KEY:
        return api_key
    
    return None