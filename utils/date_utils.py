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