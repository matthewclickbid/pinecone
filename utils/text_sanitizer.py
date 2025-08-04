import re
import html
from datetime import datetime


def format_human_readable_date(date_str):
    """
    Format date string into human readable format.
    
    Args:
        date_str (str): Date string in various formats
        
    Returns:
        str: Human readable date format like "January 1st, 2025 10:45:23 PM"
    """
    try:
        # Parse different date formats
        dt = None
        
        # Try ISO format YYYY-MM-DD
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            dt = datetime.strptime(date_str, '%Y-%m-%d')
        
        # Try ISO format with time YYYY-MM-DDTHH:MM:SS (with optional timezone)
        elif re.match(r'^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}', date_str):
            # Remove timezone info for parsing
            clean_date = re.sub(r'[TZ].*$', '', date_str.replace('T', ' ')[:19])
            dt = datetime.strptime(clean_date, '%Y-%m-%d %H:%M:%S')
        
        # Try MM/DD/YYYY format
        elif re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_str):
            dt = datetime.strptime(date_str, '%m/%d/%Y')
        
        # Try MM-DD-YYYY format
        elif re.match(r'^\d{1,2}-\d{1,2}-\d{4}$', date_str):
            dt = datetime.strptime(date_str, '%m-%d-%Y')
        
        if dt:
            # Get day suffix (1st, 2nd, 3rd, 4th, etc.)
            day = dt.day
            if 10 <= day % 100 <= 20:
                suffix = 'th'
            else:
                suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')
            
            # Format the date
            formatted_date = dt.strftime(f"%B {day}{suffix}, %Y %I:%M:%S %p")
            return formatted_date
        
        # If no pattern matches, return original
        return date_str
        
    except:
        # If parsing fails, return original string
        return date_str


def sanitize_text(text):
    """
    Sanitize text by removing HTML tags and special characters, while preserving
    dollar signs and formatting dates into human readable format.
    
    Args:
        text (str): Input text to sanitize
        
    Returns:
        str: Sanitized text with preserved dollar signs and formatted dates
    """
    if not text:
        return ""
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    
    # Decode HTML entities
    text = html.unescape(text)
    
    # Format dates into human readable format
    # Match various date patterns (ISO format, timestamps, etc.)
    date_patterns = [
        r'\b\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b',  # ISO format
        r'\b\d{4}-\d{2}-\d{2}\b',  # YYYY-MM-DD
        r'\b\d{1,2}/\d{1,2}/\d{4}\b',  # MM/DD/YYYY
        r'\b\d{1,2}-\d{1,2}-\d{4}\b',  # MM-DD-YYYY
    ]
    
    for pattern in date_patterns:
        def replace_date(match):
            return format_human_readable_date(match.group())
        text = re.sub(pattern, replace_date, text)
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Remove special characters that might cause issues, but preserve dollar signs
    text = re.sub(r'[^\w\s\-\.,!?;:()\[\]{}$]', '', text)
    
    return text