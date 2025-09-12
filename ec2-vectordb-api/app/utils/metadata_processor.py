import logging
from typing import Dict, Any, Union

logger = logging.getLogger(__name__)


def process_metadata_for_pinecone(metadata: Dict[str, Any], preserve_formatted_text: bool = True) -> Dict[str, Union[str, int, float]]:
    """
    Process metadata for Pinecone:
    1. Remove commas from all fields except formatted_text
    2. Convert numeric strings to appropriate types (int or float)
    3. Keep non-numeric values as strings
    
    Args:
        metadata: Dictionary of metadata to process
        preserve_formatted_text: If True, preserve formatted_text field as-is
        
    Returns:
        Dict with processed metadata, properly typed for Pinecone
    """
    processed = {}
    
    for key, value in metadata.items():
        # Handle None values
        if value is None:
            processed[key] = ''
            continue
        
        # Preserve formatted_text as-is if requested
        if key == 'formatted_text' and preserve_formatted_text:
            processed[key] = str(value)
            continue
        
        # Convert to string first
        str_value = str(value)
        
        # Remove commas from the value
        cleaned_value = str_value.replace(',', '')
        
        # Skip empty strings after cleaning
        if not cleaned_value:
            processed[key] = ''
            continue
        
        # Try to convert to numeric if possible
        # First try integer conversion
        try:
            # Check if it looks like an integer (handles negative numbers)
            if cleaned_value.lstrip('-').isdigit():
                processed[key] = int(cleaned_value)
                logger.debug(f"Converted field '{key}' to int: {cleaned_value}")
                continue
        except (ValueError, OverflowError):
            pass
        
        # Try float conversion
        try:
            # Check if it contains decimal point or scientific notation
            if '.' in cleaned_value or 'e' in cleaned_value.lower():
                float_val = float(cleaned_value)
                # Avoid converting integers to floats
                if float_val.is_integer() and '.' not in cleaned_value:
                    processed[key] = int(float_val)
                else:
                    processed[key] = float_val
                logger.debug(f"Converted field '{key}' to float: {cleaned_value}")
                continue
        except (ValueError, OverflowError):
            pass
        
        # Keep as string if not numeric
        processed[key] = cleaned_value
        
    return processed


def extract_namespace_from_record(record: Dict[str, Any], namespace_field: str = 'record_type') -> str:
    """
    Extract the namespace from a record based on a specific field.
    
    Args:
        record: Dictionary containing the record data
        namespace_field: Field name to use as namespace (default: 'record_type')
        
    Returns:
        Namespace string, or empty string if field not found
    """
    namespace = record.get(namespace_field, '')
    
    if namespace:
        # Clean the namespace value (remove special characters if needed)
        namespace = str(namespace).strip()
        logger.debug(f"Using namespace: {namespace}")
    else:
        logger.warning(f"No '{namespace_field}' found in record, using default namespace")
    
    return namespace