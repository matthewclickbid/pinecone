from datetime import datetime, timedelta
from dateutil.parser import parse


def parse_date(date_string):
    """
    Parse date string in YYYY-MM-DD format.
    
    Args:
        date_string (str): Date string to parse
        
    Returns:
        datetime: Parsed date object
        
    Raises:
        ValueError: If date format is invalid
    """
    try:
        return datetime.strptime(date_string, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Invalid date format. Expected YYYY-MM-DD, got: {date_string}")


def calculate_end_date(start_date_str):
    """
    Calculate end date as 2 days after start date.
    
    Args:
        start_date_str (str): Start date in YYYY-MM-DD format
        
    Returns:
        str: End date in YYYY-MM-DD format
    """
    start_date = parse_date(start_date_str)
    end_date = start_date + timedelta(days=2)
    return end_date.strftime('%Y-%m-%d')


def parse_date_range(start_date: str, end_date: str = None, default_days: int = 7):
    """
    Parse and validate date range.
    
    Args:
        start_date: Start date string in YYYY-MM-DD format
        end_date: Optional end date string in YYYY-MM-DD format
        default_days: Default number of days if end_date not provided
        
    Returns:
        tuple: (start_date_obj, end_date_obj) as datetime objects
        
    Raises:
        ValueError: If date format is invalid or range is invalid
    """
    # Parse start date
    start_date_obj = parse_date(start_date)
    
    # Calculate end date if not provided
    if end_date:
        end_date_obj = parse_date(end_date)
    else:
        end_date_obj = start_date_obj + timedelta(days=default_days)
    
    # Validate date range
    if end_date_obj < start_date_obj:
        raise ValueError(f"End date {end_date} cannot be before start date {start_date}")
    
    # Check if range is too large (optional safety check)
    max_days = 365
    if (end_date_obj - start_date_obj).days > max_days:
        raise ValueError(f"Date range cannot exceed {max_days} days")
    
    return start_date_obj, end_date_obj