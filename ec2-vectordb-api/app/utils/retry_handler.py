"""
Retry handling utilities with exponential backoff and intelligent error categorization.
"""

import logging
import time
import random
import functools
from typing import Callable, Any, Dict, List, Optional, Tuple, Type
from enum import Enum
from dataclasses import dataclass
from datetime import datetime, timedelta

from celery.exceptions import Retry
from botocore.exceptions import ClientError
from openai.error import RateLimitError, APIError, Timeout
from requests.exceptions import ConnectionError, Timeout as RequestsTimeout, HTTPError


logger = logging.getLogger(__name__)


class ErrorCategory(Enum):
    """Error categorization for intelligent retry logic."""
    TRANSIENT = "transient"  # Temporary errors, should retry
    RATE_LIMIT = "rate_limit"  # Rate limiting, should retry with backoff
    AUTHENTICATION = "authentication"  # Auth errors, should not retry
    CONFIGURATION = "configuration"  # Config errors, should not retry
    RESOURCE_NOT_FOUND = "resource_not_found"  # Missing resources, should not retry
    QUOTA_EXCEEDED = "quota_exceeded"  # Quota/billing issues, should not retry
    PERMANENT = "permanent"  # Permanent errors, should not retry
    UNKNOWN = "unknown"  # Unknown errors, should retry with caution


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 300.0  # 5 minutes
    exponential_base: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.1
    retry_on_categories: List[ErrorCategory] = None
    
    def __post_init__(self):
        if self.retry_on_categories is None:
            self.retry_on_categories = [
                ErrorCategory.TRANSIENT,
                ErrorCategory.RATE_LIMIT,
                ErrorCategory.UNKNOWN
            ]


@dataclass
class RetryAttempt:
    """Information about a retry attempt."""
    attempt_number: int
    error: Exception
    error_category: ErrorCategory
    delay: float
    timestamp: str
    total_elapsed_time: float
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow().isoformat()


class ErrorCategorizer:
    """Categorizes errors for intelligent retry logic."""
    
    @staticmethod
    def categorize_error(error: Exception) -> ErrorCategory:
        """
        Categorize an error to determine retry behavior.
        
        Args:
            error: Exception to categorize
            
        Returns:
            ErrorCategory for the error
        """
        error_type = type(error)
        error_message = str(error).lower()
        
        # OpenAI specific errors
        if isinstance(error, RateLimitError):
            return ErrorCategory.RATE_LIMIT
        
        if isinstance(error, (APIError, Timeout)):
            if "insufficient_quota" in error_message or "exceeded your current quota" in error_message:
                return ErrorCategory.QUOTA_EXCEEDED
            if "invalid_api_key" in error_message or "unauthorized" in error_message:
                return ErrorCategory.AUTHENTICATION
            return ErrorCategory.TRANSIENT
        
        # AWS/Boto3 errors
        if isinstance(error, ClientError):
            error_code = error.response.get('Error', {}).get('Code', '')
            
            if error_code in ['ThrottlingException', 'TooManyRequestsException', 'RequestLimitExceeded']:
                return ErrorCategory.RATE_LIMIT
            
            if error_code in ['InvalidCredentials', 'AccessDenied', 'UnauthorizedOperation']:
                return ErrorCategory.AUTHENTICATION
            
            if error_code in ['ResourceNotFoundException', 'NoSuchKey', 'NoSuchBucket']:
                return ErrorCategory.RESOURCE_NOT_FOUND
            
            if error_code in ['LimitExceededException', 'ServiceQuotaExceededException']:
                return ErrorCategory.QUOTA_EXCEEDED
            
            if error_code in ['InternalServerError', 'ServiceUnavailable']:
                return ErrorCategory.TRANSIENT
            
            if error_code in ['ValidationException', 'InvalidParameterValue']:
                return ErrorCategory.CONFIGURATION
        
        # HTTP/Network errors
        if isinstance(error, (ConnectionError, RequestsTimeout)):
            return ErrorCategory.TRANSIENT
        
        if isinstance(error, HTTPError):
            status_code = getattr(error.response, 'status_code', 0)
            
            if status_code == 429:  # Too Many Requests
                return ErrorCategory.RATE_LIMIT
            elif status_code in [401, 403]:  # Unauthorized/Forbidden
                return ErrorCategory.AUTHENTICATION
            elif status_code == 404:  # Not Found
                return ErrorCategory.RESOURCE_NOT_FOUND
            elif status_code >= 500:  # Server errors
                return ErrorCategory.TRANSIENT
            elif status_code >= 400:  # Client errors
                return ErrorCategory.CONFIGURATION
        
        # Generic error patterns
        if any(pattern in error_message for pattern in [
            'timeout', 'connection reset', 'connection refused', 'connection failed',
            'temporary failure', 'service unavailable', 'internal server error'
        ]):
            return ErrorCategory.TRANSIENT
        
        if any(pattern in error_message for pattern in [
            'rate limit', 'too many requests', 'throttled', 'quota exceeded'
        ]):
            return ErrorCategory.RATE_LIMIT
        
        if any(pattern in error_message for pattern in [
            'unauthorized', 'invalid credentials', 'authentication failed', 'access denied'
        ]):
            return ErrorCategory.AUTHENTICATION
        
        if any(pattern in error_message for pattern in [
            'not found', 'does not exist', 'no such'
        ]):
            return ErrorCategory.RESOURCE_NOT_FOUND
        
        return ErrorCategory.UNKNOWN


class RetryHandler:
    """
    Handles retry logic with exponential backoff and intelligent error handling.
    """
    
    def __init__(self, config: RetryConfig = None):
        self.config = config or RetryConfig()
        self.categorizer = ErrorCategorizer()
        self.retry_history: Dict[str, List[RetryAttempt]] = {}
    
    def should_retry(self, error: Exception, attempt_number: int) -> Tuple[bool, ErrorCategory]:
        """
        Determine if an error should be retried.
        
        Args:
            error: Exception that occurred
            attempt_number: Current attempt number (1-based)
            
        Returns:
            Tuple of (should_retry, error_category)
        """
        category = self.categorizer.categorize_error(error)
        
        # Check if we've exceeded max retries
        if attempt_number > self.config.max_retries:
            return False, category
        
        # Check if error category is retryable
        should_retry = category in self.config.retry_on_categories
        
        return should_retry, category
    
    def calculate_delay(self, attempt_number: int, error_category: ErrorCategory) -> float:
        """
        Calculate delay before retry based on attempt number and error category.
        
        Args:
            attempt_number: Current attempt number (1-based)
            error_category: Category of the error
            
        Returns:
            Delay in seconds
        """
        # Base exponential backoff
        delay = self.config.initial_delay * (self.config.exponential_base ** (attempt_number - 1))
        
        # Apply category-specific adjustments
        if error_category == ErrorCategory.RATE_LIMIT:
            # Longer delays for rate limiting
            delay *= 2.0
        elif error_category == ErrorCategory.TRANSIENT:
            # Standard delay for transient errors
            pass
        elif error_category == ErrorCategory.UNKNOWN:
            # Conservative delay for unknown errors
            delay *= 1.5
        
        # Apply maximum delay limit
        delay = min(delay, self.config.max_delay)
        
        # Add jitter if enabled
        if self.config.jitter:
            jitter_amount = delay * self.config.jitter_factor
            jitter = random.uniform(-jitter_amount, jitter_amount)
            delay += jitter
        
        return max(0, delay)
    
    def record_retry_attempt(self, task_id: str, attempt: RetryAttempt) -> None:
        """
        Record a retry attempt for tracking and analysis.
        
        Args:
            task_id: Task identifier
            attempt: Retry attempt information
        """
        if task_id not in self.retry_history:
            self.retry_history[task_id] = []
        
        self.retry_history[task_id].append(attempt)
        
        # Keep only recent history (last 100 attempts per task)
        if len(self.retry_history[task_id]) > 100:
            self.retry_history[task_id] = self.retry_history[task_id][-100:]
    
    def get_retry_history(self, task_id: str) -> List[RetryAttempt]:
        """
        Get retry history for a task.
        
        Args:
            task_id: Task identifier
            
        Returns:
            List of retry attempts
        """
        return self.retry_history.get(task_id, [])
    
    def clear_retry_history(self, task_id: str) -> None:
        """
        Clear retry history for a task.
        
        Args:
            task_id: Task identifier
        """
        if task_id in self.retry_history:
            del self.retry_history[task_id]


def with_retry(config: RetryConfig = None, task_id: str = None):
    """
    Decorator for adding retry logic to functions.
    
    Args:
        config: Retry configuration
        task_id: Task identifier for tracking
    
    Returns:
        Decorated function with retry logic
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retry_handler = RetryHandler(config)
            start_time = time.time()
            attempt = 0
            
            while attempt < retry_handler.config.max_retries + 1:
                attempt += 1
                
                try:
                    return func(*args, **kwargs)
                
                except Exception as error:
                    should_retry, error_category = retry_handler.should_retry(error, attempt)
                    
                    if not should_retry:
                        logger.error(f"Function {func.__name__} failed with non-retryable error: {error}")
                        raise
                    
                    if attempt > retry_handler.config.max_retries:
                        logger.error(f"Function {func.__name__} failed after {attempt-1} retries: {error}")
                        raise
                    
                    delay = retry_handler.calculate_delay(attempt, error_category)
                    total_elapsed = time.time() - start_time
                    
                    # Record retry attempt
                    if task_id:
                        retry_attempt = RetryAttempt(
                            attempt_number=attempt,
                            error=error,
                            error_category=error_category,
                            delay=delay,
                            timestamp=datetime.utcnow().isoformat(),
                            total_elapsed_time=total_elapsed
                        )
                        retry_handler.record_retry_attempt(task_id, retry_attempt)
                    
                    logger.warning(
                        f"Function {func.__name__} attempt {attempt} failed with {error_category.value} error: {error}. "
                        f"Retrying in {delay:.2f} seconds..."
                    )
                    
                    time.sleep(delay)
            
            # This should never be reached due to the logic above, but just in case
            raise RuntimeError(f"Function {func.__name__} exceeded maximum retry attempts")
        
        return wrapper
    return decorator


def celery_retry_with_backoff(task, error: Exception, attempt: int, max_retries: int = 3) -> None:
    """
    Helper function for Celery task retries with intelligent backoff.
    
    Args:
        task: Celery task instance (from bind=True)
        error: Exception that occurred
        attempt: Current attempt number
        max_retries: Maximum number of retries
    """
    retry_handler = RetryHandler(RetryConfig(max_retries=max_retries))
    
    should_retry, error_category = retry_handler.should_retry(error, attempt)
    
    if not should_retry:
        logger.error(f"Task {task.name} failed with non-retryable {error_category.value} error: {error}")
        raise error
    
    delay = retry_handler.calculate_delay(attempt, error_category)
    
    logger.warning(
        f"Task {task.name} attempt {attempt} failed with {error_category.value} error: {error}. "
        f"Retrying in {delay:.2f} seconds..."
    )
    
    # Use Celery's retry mechanism
    raise task.retry(countdown=int(delay), max_retries=max_retries, exc=error)


class CircuitBreaker:
    """
    Circuit breaker pattern implementation to prevent cascading failures.
    """
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 300, 
                 expected_exception: Type[Exception] = Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with circuit breaker protection.
        
        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            Exception: If circuit is open or function fails
        """
        if self.state == 'OPEN':
            # Check if recovery timeout has passed
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = 'HALF_OPEN'
                logger.info("Circuit breaker state changed to HALF_OPEN")
            else:
                raise Exception("Circuit breaker is OPEN - calls are blocked")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        
        except self.expected_exception as e:
            self._on_failure()
            raise e
    
    def _on_success(self) -> None:
        """Handle successful function execution."""
        if self.state == 'HALF_OPEN':
            self.state = 'CLOSED'
            self.failure_count = 0
            logger.info("Circuit breaker state changed to CLOSED")
    
    def _on_failure(self) -> None:
        """Handle failed function execution."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'
            logger.warning(f"Circuit breaker state changed to OPEN after {self.failure_count} failures")
    
    def get_state(self) -> str:
        """Get current circuit breaker state."""
        return self.state
    
    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'
        logger.info("Circuit breaker manually reset to CLOSED")


# Global retry handler instance
default_retry_handler = RetryHandler()

# Global circuit breakers for different services
openai_circuit_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=180,  # 3 minutes
    expected_exception=Exception
)

pinecone_circuit_breaker = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout=300,  # 5 minutes
    expected_exception=Exception
)

dynamodb_circuit_breaker = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout=180,  # 3 minutes
    expected_exception=ClientError
)