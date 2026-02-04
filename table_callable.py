from typing import Callable
from json_utils import extract_json_value, extract_from_array

TABLE_CALLABLES = {
    "hotel": extract_json_value,
    "confirmationNumber": extract_from_array,
    "reservationStatus": extract_from_array
}

def get_table_callable(table_name: str) -> Callable:
    print("--------------------------------")
    print("in get_table_callable")
    print(f"table name: {table_name}")

    callables = TABLE_CALLABLES.get(table_name)
    print(f"table callable: {callables}")
    return callables
