"""
JSON Path Utility Functions for AWS Glue Jobs
Handles extraction of values from JSON strings using dot-notation paths,
including support for arrays and nested arrays.
"""

import json
import re
from typing import Any, List, Union, Optional



def extract_json_value(json_str: Union[str, dict], path: str) -> Any:
    """
    Unified function to extract values from JSON using enhanced dot-notation.
    
    Supports:
    - Standard keys: 'reservation.hotel.hotelId'
    - All array items: 'reservation.reservationItems[].itemType'
    - Specific index: 'reservation.reservationCustomers[0].customer.firstName'
    - Mixed: 'reservation.reservationItems[].reservedRates[0].rateAmount'
    
    Returns:
        - A single value if the path results in one match.
        - A list of values if '[]' is used or multiple matches are found.
        - None if the path is invalid.
    """
    if not json_str:
        return None

    try:
        data = json.loads(json_str) if isinstance(json_str, str) else json_str
    except Exception as e:
        print(f"Error parsing JSON: {e}")
        return None

    # Resulting matches
    current_targets = [data]
    # Track if the user explicitly asked for an array via []
    requested_array = "[]" in path

    # Split path by dots
    parts = path.split('.')

    for part in parts:
        next_targets = []
        
        # Regex to split key from brackets: e.g., "items[0]" -> "items", "0" | "items[]" -> "items", ""
        match = re.match(r"(\w+)(?:\[(\d*)\])?", part)
        if not match:
            continue
            
        key, index_str = match.groups()

        for item in current_targets:
            if not isinstance(item, dict) or key not in item:
                continue
            
            val = item[key]
            
            if index_str == "":  # Case: key[] (All elements)
                if isinstance(val, list):
                    next_targets.extend(val)
                else:
                    next_targets.append(val)
            
            elif index_str is not None:  # Case: key[n] (Specific index)
                idx = int(index_str)
                if isinstance(val, list) and 0 <= idx < len(val):
                    next_targets.append(val[idx])
                else:
                    continue # Index out of bounds or not a list
            
            else:  # Case: simple key
                if val is not None:
                    next_targets.append(val)
        
        current_targets = next_targets

    # Return Logic:
    if not current_targets:
        return None
    
    # If the user used '[]' anywhere, they likely expect a list, 
    # even if it only contains one item.
    if requested_array:
        return current_targets
    
    # Otherwise, return single value if only one exists, else the list
    return current_targets[0] if len(current_targets) == 1 else current_targets