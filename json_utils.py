"""
JSON Path Utility Functions for AWS Glue Jobs
Handles extraction of values from JSON strings using dot-notation paths,
including support for arrays, nested arrays, and complex structures.
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
    - Nested arrays (List of Lists): 'reservation.linkedReservations[].confirmationNumber'
    
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
        print(f"ERROR: Failed to parse JSON: {str(e)}")
        print(f"JSON string (first 500 chars): {str(json_str)[:500]}")
        return None

    current_targets = [data]
    requested_array = "[]" in path
    parts = path.split('.')

    for part in parts:
        next_targets = []
        
        # Regex to split key from brackets: e.g., "items[0]" -> "items", "0" | "items[]" -> "items", ""
        match = re.match(r"(\w+)(?:\[(\d*)\])?", part)
        if not match:
            continue
            
        key, index_str = match.groups()

        # --- STEP 1: NORMALIZE TARGETS ---
        # If the previous step left us with nested lists (e.g., [[{...}], [{...}]]),
        # we flatten them so we can access the 'key' inside the dictionaries.
        normalized_targets = []
        def flatten(node):
            if isinstance(node, list):
                for item in node:
                    flatten(item)
            elif node is not None:
                normalized_targets.append(node)
        
        for t in current_targets:
            if isinstance(t, list):
                flatten(t)
            else:
                normalized_targets.append(t)

        # --- STEP 2: EXTRACT KEY ---
        for item in normalized_targets:
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
                    continue
            
            else:  # Case: simple key
                if val is not None:
                    next_targets.append(val)
        
        current_targets = next_targets

    if not current_targets:
        return None
    
    # Maintain existing return logic
    if requested_array:
        return current_targets
    
    return current_targets[0] if len(current_targets) == 1 else current_targets