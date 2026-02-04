"""
JSON Path Utility Functions for AWS Glue Jobs
Handles extraction of values from JSON strings using dot-notation paths,
including support for arrays and nested arrays.
"""

import json
from typing import Any, List


def extract_json_value(json_str: str, path: str) -> Any:
    """
    Extract a value from a JSON string using a dot-notation path.
    
    Args:
        json_str: JSON string to parse
        path: Dot-notation path (e.g., "reservation.hotel.hotelCode")
    
    Returns:
        The value at the specified path, or None if not found
    
    Examples:
        >>> json_str = '{"reservation": {"hotel": {"hotelCode": "FLF26"}}}'
        >>> extract_json_value(json_str, "reservation.hotel.hotelCode")
        'FLF26'
    """
    try:
        data = json.loads(json_str) if isinstance(json_str, str) else json_str
        return get_nested_value(data, path)
    except (json.JSONDecodeError, Exception) as e:
        print(f"Error extracting value from path '{path}': {e}")
        return None


def get_nested_value(data: Any, path: str) -> Any:
    """
    Navigate through nested dictionary/list structure using dot-notation path.
    
    Args:
        data: Dictionary or list to navigate
        path: Dot-notation path (e.g., "reservation.hotel.hotelCode")
    
    Returns:
        The value at the specified path, or None if not found
    
    Examples:
        >>> data = {"reservation": {"hotel": {"hotelCode": "FLF26"}}}
        >>> get_nested_value(data, "reservation.hotel.hotelCode")
        'FLF26'
    """
    if not path:
        return data
    
    keys = path.split('.')
    current = data
    
    for key in keys:
        if current is None:
            return None
        
        # Handle array index notation (e.g., "items[0]" or just numeric key)
        if '[' in key and ']' in key:
            # Extract key and index (e.g., "items[0]" -> "items", 0)
            key_name = key[:key.index('[')]
            index = int(key[key.index('[') + 1:key.index(']')])
            
            if isinstance(current, dict) and key_name in current:
                current = current[key_name]
                if isinstance(current, list) and 0 <= index < len(current):
                    current = current[index]
                else:
                    return None
            else:
                return None
        elif isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list):
            # If current is a list and key is numeric, treat it as index
            try:
                index = int(key)
                if 0 <= index < len(current):
                    current = current[index]
                else:
                    return None
            except ValueError:
                return None
        else:
            return None
    
    return current


def extract_from_array(json_str: str, array_path: str, field_path: str = None) -> List[Any]:
    """
    Extract all values from an array in JSON, optionally extracting a specific field from each item.
    
    Args:
        json_str: JSON string to parse
        array_path: Path to the array (e.g., "reservation.reservationItems")
        field_path: Optional path to extract from each array item (e.g., "itemType")
    
    Returns:
        List of extracted values, or empty list if array not found
    
    Examples:
        >>> json_str = '{"items": [{"name": "A"}, {"name": "B"}]}'
        >>> extract_from_array(json_str, "items", "name")
        ['A', 'B']
    """
    try:
        data = json.loads(json_str) if isinstance(json_str, str) else json_str
        array = get_nested_value(data, array_path)
        
        if not isinstance(array, list):
            return []
        
        if field_path:
            return [get_nested_value(item, field_path) for item in array if item is not None]
        else:
            return array
    except Exception as e:
        print(f"Error extracting array from path '{array_path}': {e}")
        return []


def extract_all_matching_values(json_str: str, path: str) -> List[Any]:
    """
    Extract all values matching a path that may traverse arrays at any level.
    Automatically expands arrays in the path.
    
    Args:
        json_str: JSON string to parse
        path: Dot-notation path (e.g., "reservation.reservationItems.reservedRates.rateAmount")
    
    Returns:
        List of all matching values (flattened if multiple arrays in path)
    
    Examples:
        >>> json_str = '{"items": [{"prices": [{"amount": 10}, {"amount": 20}]}]}'
        >>> extract_all_matching_values(json_str, "items.prices.amount")
        [10, 20]
    """
    try:
        data = json.loads(json_str) if isinstance(json_str, str) else json_str
        return _recursive_extract(data, path.split('.'))
    except Exception as e:
        print(f"Error extracting all matching values from path '{path}': {e}")
        return []


def _recursive_extract(current: Any, keys: List[str]) -> List[Any]:
    """
    Recursively extract values, expanding arrays automatically.
    
    Args:
        current: Current data structure
        keys: Remaining path keys to traverse
    
    Returns:
        List of all matching values
    """
    if not keys:
        return [current] if current is not None else []
    
    if current is None:
        return []
    
    key = keys[0]
    remaining_keys = keys[1:]
    results = []
    
    # Handle array index notation
    if '[' in key and ']' in key:
        key_name = key[:key.index('[')]
        index = int(key[key.index('[') + 1:key.index(']')])
        
        if isinstance(current, dict) and key_name in current:
            array = current[key_name]
            if isinstance(array, list) and 0 <= index < len(array):
                results.extend(_recursive_extract(array[index], remaining_keys))
    # Handle dictionary
    elif isinstance(current, dict):
        if key in current:
            next_value = current[key]
            # If next value is a list, expand it
            if isinstance(next_value, list):
                for item in next_value:
                    results.extend(_recursive_extract(item, remaining_keys))
            else:
                results.extend(_recursive_extract(next_value, remaining_keys))
    # Handle list - expand all items
    elif isinstance(current, list):
        for item in current:
            # Re-process with the same key for each item
            results.extend(_recursive_extract(item, keys))
    
    return results


def extract_first_matching_value(json_str: str, path: str) -> Any:
    """
    Extract the first value matching a path (useful when you only need one value from arrays).
    
    Args:
        json_str: JSON string to parse
        path: Dot-notation path
    
    Returns:
        First matching value, or None if not found
    
    Examples:
        >>> json_str = '{"items": [{"name": "A"}, {"name": "B"}]}'
        >>> extract_first_matching_value(json_str, "items.name")
        'A'
    """
    results = extract_all_matching_values(json_str, path)
    return results[0] if results else None


def safe_extract(json_str: str, path: str, default: Any = None) -> Any:
    """
    Safely extract a value with a default fallback.
    
    Args:
        json_str: JSON string to parse
        path: Dot-notation path
        default: Default value to return if extraction fails
    
    Returns:
        Extracted value or default
    """
    result = extract_json_value(json_str, path)
    return result if result is not None else default


# PySpark UDF-compatible functions
def create_extract_udf(path: str):
    """
    Create a function suitable for use as a PySpark UDF.
    
    Args:
        path: The JSON path to extract
    
    Returns:
        A function that takes a JSON string and returns the extracted value
    
    Example usage in PySpark:
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        
        extract_hotel_code = create_extract_udf("reservation.hotel.hotelCode")
        hotel_code_udf = udf(extract_hotel_code, StringType())
        
        df = df.withColumn("hotel_code", hotel_code_udf(df["raw"]))
    """
    def extract_func(json_str):
        return extract_json_value(json_str, path)
    return extract_func


def create_extract_array_udf(array_path: str, field_path: str = None):
    """
    Create a function to extract array values, suitable for PySpark UDF.
    
    Args:
        array_path: Path to the array
        field_path: Optional field to extract from each array item
    
    Returns:
        A function that takes a JSON string and returns a list of values
    
    Example usage in PySpark:
        from pyspark.sql.functions import udf
        from pyspark.sql.types import ArrayType, StringType
        
        extract_item_types = create_extract_array_udf(
            "reservation.reservationItems", 
            "itemType"
        )
        item_types_udf = udf(extract_item_types, ArrayType(StringType()))
        
        df = df.withColumn("item_types", item_types_udf(df["raw"]))
    """
    def extract_func(json_str):
        return extract_from_array(json_str, array_path, field_path)
    return extract_func