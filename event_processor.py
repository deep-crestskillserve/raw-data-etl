from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, Callable, Any, Optional, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from json_utils import get_nested_value, extract_json_value, extract_from_array, extract_all_matching_values, extract_first_matching_value
from typing import List
from pyspark.sql.types import ArrayType, StringType, IntegerType, DecimalType, DateType, TimestampType, BooleanType, BinaryType, LongType
from pyspark.sql.types import StructField, StringType, BooleanType, IntegerType, DecimalType, DateType, TimestampType
import json
import uuid
import hashlib
from pyspark.sql.functions import sha2, concat_ws, col, expr
from business_keys import get_business_keys
from pyspark_schema import get_table_schema
from pyspark.sql.functions import when, concat, upper, substring, regexp_extract, lit
from event_wise_tables import get_event_table_mappings
from table_callable import get_table_callable
from functools import reduce
from decimal import Decimal, InvalidOperation
from datetime import datetime, date


UUID_NAMESPACE = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')


def safe_type_conversion(value, target_type):
    """
    Safely convert a value to the target PySpark data type.
    Returns None for invalid/missing values - let validation handle nullability.
    
    KEY PRINCIPLE: This function returns None for invalid data.
    The validate_schema_nullability() function will then move records with
    None in non-nullable columns to the failed_accumulator.
    
    This ensures:
    1. Database gets proper NULL values (not empty strings)
    2. Data quality is maintained through validation
    3. Business logic can distinguish between NULL and empty string
    """
    if value is None:
        return None
    
    try:
        if isinstance(target_type, StringType):
            # For String fields, we still need special handling for JVM serialization
            # BUT we handle it differently - see safe_batch_extract() below
            if isinstance(value, (list, dict)):
                return json.dumps(value)
            if isinstance(value, str):
                # Return None for truly empty/whitespace strings
                stripped = value.strip()
                if not stripped or stripped.lower() in ('null', 'none', 'n/a'):
                    return None
                return stripped
            return str(value)
        
        elif isinstance(target_type, IntegerType):
            # Integer: Convert to int, handle various formats
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, str):
                value = value.strip()
                if not value or value.lower() in ('null', 'none', 'n/a', ''):
                    return None
                # Remove commas
                value = value.replace(',', '')
                # Handle decimal strings by truncating
                if '.' in value:
                    return int(float(value))
                return int(value)
            if isinstance(value, (int, float)):
                return int(value)
            return None
        
        elif isinstance(target_type, LongType):
            # Long: Similar to Integer but for larger numbers
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, str):
                value = value.strip()
                if not value or value.lower() in ('null', 'none', 'n/a', ''):
                    return None
                value = value.replace(',', '')
                if '.' in value:
                    return int(float(value))
                return int(value)
            if isinstance(value, (int, float)):
                return int(value)
            return None
        
        elif isinstance(target_type, DecimalType):
            # Decimal: Handle monetary values with precision
            if isinstance(value, str):
                value = value.strip()
                if not value or value.lower() in ('null', 'none', 'n/a', ''):
                    return None
                # Remove currency symbols and commas
                value = value.replace('$', '').replace(',', '').replace('€', '').replace('£', '')
            if isinstance(value, bool):
                return Decimal(str(int(value)))
            if isinstance(value, (int, float)):
                return Decimal(str(value))
            if isinstance(value, str):
                try:
                    return Decimal(value)
                except InvalidOperation:
                    return None
            return None
        
        elif isinstance(target_type, BooleanType):
            # Boolean: Handle various representations
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                value = value.strip().lower()
                if value in ('true', 't', 'yes', 'y', '1', 'on'):
                    return True
                elif value in ('false', 'f', 'no', 'n', '0', 'off'):
                    return False
                elif value in ('', 'null', 'none', 'n/a'):
                    return None
                return None
            if isinstance(value, (int, float)):
                return bool(value)
            return None
        
        elif isinstance(target_type, DateType):
            # Date: Handle various date formats
            if isinstance(value, date):
                return value
            if isinstance(value, datetime):
                return value.date()
            if isinstance(value, str):
                value = value.strip()
                if not value or value.lower() in ('null', 'none', 'n/a', ''):
                    return None
                # Try common date formats
                date_formats = [
                    '%Y-%m-%d',
                    '%Y/%m/%d',
                    '%d-%m-%Y',
                    '%d/%m/%Y',
                    '%m-%d-%Y',
                    '%m/%d/%Y',
                    '%Y%m%d',
                ]
                for fmt in date_formats:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
                # Try ISO format parsing
                try:
                    return datetime.fromisoformat(value.replace('Z', '+00:00')).date()
                except:
                    return None
            return None
        
        elif isinstance(target_type, TimestampType):
            # Timestamp: Handle various datetime formats
            if isinstance(value, datetime):
                return value
            if isinstance(value, date):
                return datetime.combine(value, datetime.min.time())
            if isinstance(value, str):
                value = value.strip()
                if not value or value.lower() in ('null', 'none', 'n/a', ''):
                    return None
                # Try common timestamp formats
                timestamp_formats = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%dT%H:%M:%S',
                    '%Y-%m-%d %H:%M:%S.%f',
                    '%Y-%m-%dT%H:%M:%S.%f',
                    '%Y/%m/%d %H:%M:%S',
                    '%d-%m-%Y %H:%M:%S',
                    '%m-%d-%Y %H:%M:%S',
                ]
                for fmt in timestamp_formats:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
                # Try ISO format with timezone
                try:
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                except:
                    # Last resort: try parsing just the date part
                    try:
                        return datetime.strptime(value[:10], '%Y-%m-%d')
                    except:
                        return None
            if isinstance(value, (int, float)):
                # Assume Unix timestamp (seconds since epoch)
                try:
                    return datetime.fromtimestamp(value)
                except:
                    return None
            return None
        
        elif isinstance(target_type, BinaryType):
            # Binary: Handle byte data
            if isinstance(value, bytes):
                return value
            if isinstance(value, str):
                value = value.strip()
                if not value or value.lower() in ('null', 'none', 'n/a', ''):
                    return None
                # Try to decode if it's a hex string or base64
                try:
                    return bytes.fromhex(value)
                except:
                    try:
                        import base64
                        return base64.b64decode(value)
                    except:
                        # Last resort: encode as UTF-8
                        return value.encode('utf-8')
            return None
        
        else:
            # Unknown type: return as-is
            return value
            
    except Exception as e:
        # If conversion fails, return None
        return None


def safe_batch_extract(data, field_mapping, udf_schema, extract_func):
    """
    IMPROVED APPROACH: Return proper None values, but handle String JVM serialization.
    
    Strategy:
    1. For all types: Return None if conversion fails (proper NULL handling)
    2. For StringType specifically: Use a MARKER VALUE to avoid JVM NullPointerException
    3. After UDF execution: Replace marker with actual None
    4. Let validate_schema_nullability() filter records with None in non-nullable fields
    
    This ensures:
    - Database gets proper NULL values (not empty strings)
    - No JVM serialization errors
    - Proper data quality through validation
    """
    if not data:
        return None
    
    # MARKER for String None values to avoid JVM serialization issues
    STRING_NULL_MARKER = "___NULL___"
    
    results = {}
    for field in udf_schema.fields:
        path = field_mapping.get(field.name)
        raw_value = extract_func(data, path)
        
        # Use safe type conversion for all types
        converted_value = safe_type_conversion(raw_value, field.dataType)
        
        # CRITICAL FIX: For StringType with None, use marker to avoid JVM NPE
        if isinstance(field.dataType, StringType) and converted_value is None:
            converted_value = STRING_NULL_MARKER
        
        results[field.name] = converted_value
    
    return results


@F.udf(returnType=StringType())
def to_uuid_v5(hash_str):
    if not hash_str:
        return None
    return str(uuid.uuid5(UUID_NAMESPACE, str(hash_str)))


class BaseEventProcessor:
    def __init__(self, spark: SparkSession, audit_dfs: Dict[str, DataFrame]):
        self.spark = spark
        self.audit_dfs = audit_dfs or {}
        
        # Define the marker for String NULL values
        self.STRING_NULL_MARKER = "___NULL___"

    def validate_schema_nullability(self, df: DataFrame, schema: StructType) -> Tuple[DataFrame, DataFrame]:
        """
        Splits the dataframe into valid and failed records based on schema nullability.
        
        IMPROVED: Properly handles NULL values across all types.
        Records with NULL in non-nullable columns go to failed_accumulator.
        """
        # Identify columns that cannot be null
        non_nullable_cols = [f.name for f in schema.fields if not f.nullable]
        
        if not non_nullable_cols:
            return df, self.spark.createDataFrame([], schema)

        # Build filter condition: True if any non-nullable column is null
        null_conditions = []
        
        for col_name in non_nullable_cols:
            field = next((f for f in schema.fields if f.name == col_name), None)
            if not field:
                continue
            
            # Base condition: column is null
            condition = F.col(col_name).isNull()
            
            # For String columns: also check for marker value (which represents NULL)
            if isinstance(field.dataType, StringType):
                condition = condition | (F.col(col_name) == self.STRING_NULL_MARKER)
            
            # For Decimal columns: also check for NaN and Infinity
            elif isinstance(field.dataType, DecimalType):
                condition = condition | F.isnan(F.col(col_name)) | \
                           (F.col(col_name) == float('inf')) | \
                           (F.col(col_name) == float('-inf'))
            
            null_conditions.append(condition)
        
        if null_conditions:
            null_condition = reduce(lambda a, b: a | b, null_conditions)
        else:
            # No conditions, return all as valid
            return df, self.spark.createDataFrame([], schema)
        
        failed_df = df.filter(null_condition)
        valid_df = df.filter(~null_condition)
        
        return valid_df, failed_df
    
    def finalize_and_audit(self, df: DataFrame, table_name: str, full_schema: StructType) -> Dict[str, DataFrame]:
        """
        Returns a dict: {"succeeded": DataFrame, "failed": DataFrame}
        
        IMPROVED: Replaces String NULL markers with actual None before validation.
        """
        print("--------------------------------")
        print("in finalize_and_audit")
        print(f"table name: {table_name}")

        # STEP 1: Replace String NULL markers with actual None
        for field in full_schema.fields:
            if isinstance(field.dataType, StringType) and field.name in df.columns:
                print(f"Replacing NULL markers in column: {field.name}")
                df = df.withColumn(
                    field.name,
                    F.when(
                        F.col(field.name) == self.STRING_NULL_MARKER,
                        F.lit(None)
                    ).otherwise(F.col(field.name))
                )

        # STEP 2: Map Metadata
        df = df.withColumn("source_id", F.col("id"))
        
        # If table is reservationStatus, use statusTimestamp as received_at
        if table_name == "reservationStatus":
            df.show(20, truncate=False)
            timestamp_col = "statusTimestamp"
        else:
            timestamp_col = "event_timestamp"

        df = df.withColumn("received_at", F.col(timestamp_col)).filter(F.col("received_at").isNotNull())

        # STEP 3: Generate Record Hash & UUID
        business_keys = get_business_keys(table_name)
        df = self.generate_record_hash(df, business_keys)

        df = df.withColumn("id", to_uuid_v5(F.col("sha256_hash"))).drop("sha256_hash")

        df = df.filter(F.col("id").isNotNull())

        # STEP 4: Match schema and cast types with error handling
        select_expressions = []
        for field in full_schema.fields:
            if field.name in df.columns:
                # Column exists: cast it to the correct type from the schema
                if isinstance(field.dataType, (DecimalType, DateType, TimestampType)):
                    # For these types, use safe casting
                    select_expressions.append(
                        F.when(
                            F.col(field.name).isNotNull(),
                            F.col(field.name).cast(field.dataType)
                        ).otherwise(F.lit(None).cast(field.dataType)).alias(field.name)
                    )
                else:
                    select_expressions.append(F.col(field.name).cast(field.dataType).alias(field.name))
            else:
                # Column does not exist: add as typed NULL
                select_expressions.append(F.lit(None).cast(field.dataType).alias(field.name))

        final_df = df.select(*select_expressions)
        print("--------------------------------")
        if table_name == "reservationStatus":
            print("final_df")
            final_df.show(20, truncate=False)
        print("--------------------------------")
        
        # STEP 5: VALIDATION - Split based on nullability
        # This will move records with NULL in non-nullable columns to failed_df
        valid_df, failed_df = self.validate_schema_nullability(final_df, full_schema)

        # STEP 6: AUDIT - Only audit valid records
        audited_valid_df = self.audit_processor(table_name, valid_df)

        return {
            "succeeded": audited_valid_df,
            "failed": failed_df
        }
    
    def process_dataframe(self, df: DataFrame, field_mapping: Any, extract_func: Callable, table_name: str) -> dict[str, DataFrame]:
        """
        Extracts fields, generates hash and UUIDv5, and enforces the full table schema.
        """
        # Guard: If field_mapping is None, we can't iterate to build the schema
        if field_mapping is None or not field_mapping:
            print(f"CRITICAL: No mapping found for table {table_name}. Returning empty DF.")
            empty_df = self.spark.createDataFrame([], get_table_schema(table_name))
            return {"succeeded": empty_df, "failed": empty_df}

        is_array_table = table_name in ["confirmationNumber", "reservationStatus"]

        if isinstance(field_mapping, list):
            merged_mapping = {}
            for m in field_mapping:
                if isinstance(m, dict): merged_mapping.update(m)
            field_mapping = merged_mapping
        
        print("--------------------------------")
        print("in process_dataframe")
        print(f"table: {table_name}")

        if is_array_table:
            processed_df = self._extract_array(df, field_mapping, table_name)
        else:
            processed_df = self._extract_single(df, field_mapping, extract_func, table_name)
        
        return self.finalize_and_audit(processed_df, table_name, get_table_schema(table_name))

    def _extract_single(self, df: DataFrame, field_mapping: Any, extract_func: Callable, table_name: str) -> DataFrame:
        
        print("--------------------------------")
        print("in _extract_single")

        full_schema = get_table_schema(table_name)

        if not callable(extract_func):
            print(f"CRITICAL: extract_func for {table_name} is not callable. Check table_callable.py")
            return self.spark.createDataFrame([], full_schema)

        udf_fields = [field for field in full_schema.fields if field.name in field_mapping]
        udf_schema = StructType(udf_fields)

        @F.udf(returnType=udf_schema)
        def batch_extract_udf(raw_str):
            if not raw_str: 
                return None
            try:
                data = json.loads(raw_str) if isinstance(raw_str, str) else raw_str
                return safe_batch_extract(data, field_mapping, udf_schema, extract_func)
            except Exception as e:
                # Log the error for debugging but return None to avoid crashing
                print(f"Error in batch_extract_udf: {str(e)}")
                return None
        
        df = df.withColumn("extracted_data", batch_extract_udf(F.col("raw")))
        
        # Extract each field from the struct
        for field in udf_schema.fields:
            df = df.withColumn(field.name, F.col(f"extracted_data.{field.name}"))
        
        df = df.drop("extracted_data")

        return df
    
    def _extract_array(self, df: DataFrame, field_mapping: Any, table_name: str) -> DataFrame:
        """
        Special handler for tables that come from JSON arrays (1 event -> multiple rows)
        """

        print("--------------------------------")
        print("in _extract_array")

        # 1. Identify the array path (e.g. reservation.reservationConfirmationNumbers)
        # We take the parent path from the first mapping entry
        first_mapping_path = list(field_mapping.values())[0]
        array_path_parts = first_mapping_path.split('.')

        print(f"array path: {first_mapping_path}")
        print(f"array path parts: {array_path_parts}")

        if len(array_path_parts) > 1:
            array_json_path = ".".join(array_path_parts[:-1])
        else:
            array_json_path = ""
        
        # 2. Extract the array string
        json_path_query = "$" if array_json_path == "" else f"$.{array_json_path}"
        df = df.withColumn("temp_array_str", F.get_json_object(F.col("raw"), json_path_query))

        
        # 3. Create a UDF to convert the JSON array string into a Python list of JSON object strings
        @F.udf(returnType=ArrayType(StringType()))
        def json_array_to_list(array_str):
            if not array_str: return []
            try:
                data = json.loads(array_str)
                return [json.dumps(obj) for obj in data] if isinstance(data, list) else []
            except: return []


        df = df.withColumn("exploded_json_list", json_array_to_list(F.col("temp_array_str")))
        df = df.withColumn("single_row_json", F.explode(F.col("exploded_json_list")))
        
        # 4. Extract fields from the exploded individual JSON objects
        for col_name, path in field_mapping.items():
            leaf_key = path.split('.')[-1]
            df = df.withColumn(col_name, F.get_json_object(F.col("single_row_json"), f"$.{leaf_key}"))
        
        df = df.drop("temp_array_str", "exploded_json_list", "single_row_json")
        return df    

    def generate_record_hash(self, df: DataFrame, business_fields: list, hash_column_name: str = "sha256_hash") -> DataFrame:
        """
        Generate a hash for each record in the dataframe based on specified business fields.
        
        Parameters:
        -----------
        df : DataFrame
            Input PySpark DataFrame
        business_fields : list
            List of column names to include in hash generation
        hash_column_name : str, optional
            Name for the hash column (default: "sha256_hash")
        
        Returns:
        --------
        DataFrame
            DataFrame with an additional hash column
        """
        # Safety Guard: Ensure business_fields is iterable
        if not business_fields:
            business_fields = ["source_id"]
        
        # Filter business fields to only those that exist in the DF
        valid_fields = [f for f in business_fields if f in df.columns]
        if not valid_fields:
            valid_fields = ["source_id"]
        
        concat_expr = concat_ws("|", *[col(field).cast("string") for field in valid_fields])
        return df.withColumn(hash_column_name, sha2(concat_expr, 256))

    def audit_processor(self, table_name: str, table_df: DataFrame) -> DataFrame:
        """
        Filters out records that already exist in the audit dataframe.
        
        Args:
            table_name: Name of the table to look up in audit_dfs
            table_df: The newly processed records
            
        Returns:
            DataFrame containing only records not present in the audit table
        """
        # 1. Retrieve the existing audit data for this table
        audit_df = self.audit_dfs.get(table_name)
        if audit_df is None:
            return table_df

        # 3. Perform a Left Anti Join
        # We use the deterministic 'id' (the UUID generated from business keys) 
        # to identify unique records.
        # This keeps only rows in table_df that DO NOT exist in audit_df.
        new_records_df = table_df.join(
            audit_df.select("id"),  # Only select the ID to keep the join slim
            on="id", 
            how="left_anti"
        )

        return new_records_df
    
    def hotel_processor(self, event_type: str, event_df: DataFrame, extract_func: Callable) -> Dict[str, DataFrame]:
        print("--------------------------------")
        print("in hotel_processor")
        print(f"event type: {event_type}")

        hotel_fields = get_event_table_mappings(event_type, "hotel")
        print(f"Hotel fields: {hotel_fields}")
        
        # 1. Get hotel dataframe with audited unique records
        extracted_hotel_df = self.process_dataframe(event_df, hotel_fields, extract_func, table_name="hotel")
        
        # 2. Apply Property Name Abbreviation Logic
        def abbreviate_name(df):
            if "propertyName" not in df.columns: return df
            return df.withColumn("propertyName_abbrev", 
                F.when(F.col("propertyName").rlike("^[A-Za-z]+ [A-Za-z]+ "),
                F.concat(F.upper(F.substring(F.regexp_extract(F.col("propertyName"), r"^([A-Za-z]+)", 1), 1, 1)),
                         F.upper(F.substring(F.regexp_extract(F.col("propertyName"), r"^[A-Za-z]+ ([A-Za-z]+)", 1), 1, 1)),
                         F.lit(" "), F.regexp_extract(F.col("propertyName"), r"^[A-Za-z]+ [A-Za-z]+ (.+)", 1))
                ).otherwise(F.col("propertyName")))
        
        return {
            "succeeded": abbreviate_name(extracted_hotel_df["succeeded"]),
            "failed": abbreviate_name(extracted_hotel_df["failed"])
        }
    
    def generic_table_processor(self, event_type: str, table_name: str, event_df: DataFrame, extract_func: Callable) -> Dict[str, DataFrame]:
        print("--------------------------------")
        print("in generic_table_processor")
        print(f"event type: {event_type}")
        print(f"table name: {table_name}")

        table_fields = get_event_table_mappings(event_type, table_name)
        return self.process_dataframe(event_df, table_fields, extract_func, table_name)
    
    def reservation_checkout_confirmation_number_processor(self, event_type: str, event_df: DataFrame, extract_func: Callable) -> Dict[str, DataFrame]:
        # 1. Get the mappings
        mappings = get_event_table_mappings(event_type, "confirmationNumber")
        
        # 2. Extract DataFrames into a list
        # Guard: Ensure mappings is a list
        if not isinstance(mappings, list):
            mappings = [mappings] if mappings else []

        s_list = []
        f_list = []
        for mapping in mappings:
            if mapping: # Guard against None entries in list
                res = self.process_dataframe(event_df, mapping, extract_func, table_name="confirmationNumber")
                s_list.append(res["succeeded"])
                f_list.append(res["failed"])


        # 3. Handle empty list case (to avoid errors)
        empty = self.spark.createDataFrame([], get_table_schema("confirmationNumber"))

        return {
            "succeeded": reduce(lambda d1, d2: d1.unionByName(d2), s_list) if s_list else empty,
            "failed": reduce(lambda d1, d2: d1.unionByName(d2), f_list) if f_list else empty
        }

def handle_null_values(df: DataFrame) -> DataFrame:
    """
    Pre-processes the input DataFrame to handle potential nulls 
    in the core columns before UDF processing.
    """
    if df is None:
        return df
    # Ensure 'raw' is never null for the UDFs; fill with empty JSON object if missing
    # and drop rows where 'id' or 'event_name' might be critically missing
    return df.na.fill({"raw": "{}"}).filter(F.col("raw").isNotNull())

def process_events_by_type(spark: SparkSession, processed_df: DataFrame, audit_dfs: Dict[str, DataFrame]) -> Tuple[Dict[str, DataFrame], dict[str, DataFrame]]:
    """
    Process events based on their event_name type.
    Each event type gets processed by its corresponding processor function.
    
    Args:
        processed_df: DataFrame with columns [id, event_timestamp, event_name, raw]
        audit_dfs: Dictionary of audit dataframes
    Returns:
        Unified DataFrame with processed results from all event types
    """
    
    # Define processor mapping
    # Guard: Ensure audit_dfs is not None
    safe_audit_dfs = audit_dfs if audit_dfs is not None else {}

    processors = {
        "reservationCreate": ReservationCreateModifyProcessor,
        "reservationModify": ReservationCreateModifyProcessor,
        "reservationRoomAssigned": ReservationRoomAssignedProcessor,
        "reservationCancel": ReservationCancelProcessor,
        "reservationCheckIn": ReservationCheckInProcessor,
        "reservationECheckIn": ReservationECheckInProcessor,
        "reservationCheckOut": ReservationCheckOutProcessor,
        "reservationEmailConfirmation": ReservationEmailConfirmationProcessor,
        "reservationRoomChange": ReservationRoomChangeProcessor,
        "housekeepingStatus": housekeepingStatusProcessor,
        "groupCreate": GroupCreateModifyProcessor,
        "groupModify": GroupCreateModifyProcessor,
        "groupCheckOut": GroupCheckOutProcessor,
        "groupCancel": GroupCancelProcessor,
        "inventoryUpdate": InventoryUpdateProcessor,
        "inventoryBatch": InventoryBatchProcessor,
        "appliedRateUpdate": AppliedRateUpdateProcessor,
        "bestAvailableRateUpdate": BestAvailableRateUpdateProcessor,
        "discountRateUpdate": DiscountRateUpdateProcessor,
        "fixedRateUpdate": FixedRateUpdateProcessor
    }
    
    # Get unique event types present in the dataframe
    event_types_rows = processed_df.select("event_name").distinct().collect()
    event_types = [row.event_name for row in event_types_rows if row.event_name is not None]
    
    # Process each event type separately and collect results
    success_accumulator = {}
    failed_accumulator = {}
    
    
    for event_type in event_types:
        processor_class = processors.get(event_type)
        if not processor_class: continue

        event_df = processed_df.filter(F.col("event_name") == event_type)
        processor_instance = processor_class(spark, safe_audit_dfs)

        # Process with corresponding processor
        result_dict = processor_instance.process(event_df)
        
        # Collect the processed dataframe, perform left anti join with the existing processed dataframe to get unique records
        
        # Guard: ensure result_dict is iterable
        if result_dict is None or not isinstance(result_dict, dict):
            print(f"WARNING: Processor for {event_type} returned None or non-dict. Skipping.")
            continue

        for table_name, split_dfs in result_dict.items():
            # result_dict[table_name] is {"succeeded": df, "failed": df}
            s_df = split_dfs.get("succeeded")
            f_df = split_dfs.get("failed")
            if s_df is not None:
                if table_name not in success_accumulator:
                    success_accumulator[table_name] = s_df
                else:
                    success_accumulator[table_name] = success_accumulator[table_name].unionByName(s_df)
            
            if f_df is not None:
                if table_name not in failed_accumulator:
                    failed_accumulator[table_name] = f_df
                else:
                    failed_accumulator[table_name] = failed_accumulator[table_name].unionByName(f_df)
    
    # FINAL DEDUPLICATION: Pick one record per ID across all unions
    # Since these are hotel records, we usually want the LATEST one 
    # (the one with the most recent received_at)

    final_success = {}
    for table_name, table_df in success_accumulator.items():
        # Sort by received_at descending so the newest record is on top, then drop duplicates
        # Only process if we actually have rows
        # Filter out null IDs and received_at BEFORE attempting count
        safe_df = table_df.filter(
            (F.col("id").isNotNull()) & 
            (F.col("received_at").isNotNull())
        )
        
        # Use try-except to handle any remaining NPE issues gracefully
        try:
            if safe_df.limit(1).count() > 0: 
                final_success[table_name] = safe_df.orderBy(F.col("received_at").desc()).dropDuplicates(["id"])
        except Exception as e:
            print(f"WARNING: Error processing {table_name} in success_accumulator: {e}")
            # Skip this table if count fails
            continue
    
    final_failed = {}
    for table_name, table_df in failed_accumulator.items():
        # Sort by received_at descending so the newest record is on top, then drop duplicates
        # Filter out null IDs and received_at BEFORE attempting count
        safe_df = table_df.filter(
            (F.col("id").isNotNull()) & 
            (F.col("received_at").isNotNull())
        )
        
        # Use try-except to handle any remaining NPE issues gracefully
        try:
            if safe_df.limit(1).count() > 0:
                final_failed[table_name] = safe_df.orderBy(F.col("received_at").desc()).dropDuplicates(["id"])
        except Exception as e:
            print(f"WARNING: Error processing {table_name} in failed_accumulator: {e}")
            # Skip this table if count fails
            continue

    return final_success, final_failed

class ReservationCreateModifyProcessor(BaseEventProcessor):
    """Processor for reservationCreate and reservationModify events."""

    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        event_type = "reservationCreate"
        print("--------------------------------")
        print("in ReservationCreateModifyProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df, get_table_callable("confirmationNumber"))
        reservation_status_df = self.generic_table_processor(event_type, "reservationStatus", event_df, get_table_callable("reservationStatus"))

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "reservationStatus": reservation_status_df,
        }

class ReservationCancelProcessor(BaseEventProcessor):
    """Processor for reservationCancel events."""

    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:

        event_type = "reservationCancel"
        print("--------------------------------")
        print("in ReservationCancelProcessor")
        print(f"event type: {event_type}")
        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df, get_table_callable("confirmationNumber"))

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
        }

class ReservationCheckInProcessor(BaseEventProcessor):
    """Processor for reservationCheckIn events."""
    
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
    
        event_type = "reservationCheckIn"
        print("--------------------------------")
        print("in ReservationCheckInProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df, get_table_callable("confirmationNumber"))
        reservation_status_df = self.generic_table_processor(event_type, "reservationStatus", event_df, get_table_callable("reservationStatus"))

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "reservationStatus": reservation_status_df
        }

class ReservationCheckOutProcessor(BaseEventProcessor):
    """Processor for reservationCheckOut events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:

        event_type = "reservationCheckOut"
        print("--------------------------------")
        print("in ReservationCheckOutProcessor")
        print(f"event type: {event_type}")
        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        confirmation_number_df = self.reservation_checkout_confirmation_number_processor(event_type, event_df, get_table_callable("confirmationNumber"))
        reservation_status_df = self.generic_table_processor(event_type, "reservationStatus", event_df, get_table_callable("reservationStatus"))
        
        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "reservationStatus": reservation_status_df
        }

class ReservationRoomAssignedProcessor(BaseEventProcessor):
    """Processor for reservationRoomAssigned events."""

    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:

        event_type = "reservationRoomAssigned"
        print("--------------------------------")
        print("in ReservationRoomAssignedProcessor")
        print(f"event type: {event_type}")
        
        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df, get_table_callable("confirmationNumber"))
        reservation_status_df = self.generic_table_processor(event_type, "reservationStatus", event_df, get_table_callable("reservationStatus"))

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "reservationStatus": reservation_status_df
        }

class ReservationRoomChangeProcessor(BaseEventProcessor):
    """Processor for reservationRoomChange events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "reservationRoomChange"
        print("--------------------------------")
        print("in ReservationRoomChangeProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df, get_table_callable("confirmationNumber"))
        reservation_status_df = self.generic_table_processor(event_type, "reservationStatus", event_df, get_table_callable("reservationStatus"))
        
        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "reservationStatus": reservation_status_df
        }
    
class ReservationECheckInProcessor(BaseEventProcessor):
    """Processor for reservationECheckIn events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "reservationECheckIn"
        print("--------------------------------")
        print("in ReservationECheckInProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df, get_table_callable("confirmationNumber"))
        reservation_status_df = self.generic_table_processor(event_type, "reservationStatus", event_df, get_table_callable("reservationStatus"))

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "reservationStatus": reservation_status_df
        }

class ReservationEmailConfirmationProcessor(BaseEventProcessor):
    """Processor for reservationEmailConfirmation events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:

        event_type = "reservationEmailConfirmation"
        print("--------------------------------")
        print("in ReservationEmailConfirmationProcessor")
        print(f"event type: {event_type}")
        
        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df, get_table_callable("confirmationNumber"))

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
        }

class GroupCreateModifyProcessor(BaseEventProcessor):
    """Processor for groupCreate and groupModify events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "groupCreate"
        print("--------------------------------")
        print("in GroupCreateModifyProcessor")
        print(f"event type: {event_type}")
        
        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df, get_table_callable("confirmationNumber"))

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
        }

class GroupCancelProcessor(BaseEventProcessor):
    """Processor for groupCancel events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "groupCancel"
        print("--------------------------------")
        print("in GroupCancelProcessor")
        print(f"event type: {event_type}")
        
        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df, get_table_callable("confirmationNumber"))

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
        }

class GroupCheckOutProcessor(BaseEventProcessor):
    """Processor for groupCheckOut events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "groupCheckOut"
        print("--------------------------------")
        print("in GroupCheckOutProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df, get_table_callable("confirmationNumber"))

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
        }

class AppliedRateUpdateProcessor(BaseEventProcessor):
    """Processor for appliedRateUpdate events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "appliedRateUpdate"
        print("--------------------------------")
        print("in AppliedRateUpdateProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        
        return {
            "hotel": hotel_df,
        }

class DiscountRateUpdateProcessor(BaseEventProcessor):
    """Processor for discountRateUpdate events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:

        event_type = "discountRateUpdate"
        print("--------------------------------")
        print("in DiscountRateUpdateProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        
        return {
            "hotel": hotel_df,
        }

class FixedRateUpdateProcessor(BaseEventProcessor):
    """Processor for fixedRateUpdate events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "fixedRateUpdate"
        print("--------------------------------")
        print("in FixedRateUpdateProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        
        return {
            "hotel": hotel_df,
        }

class BestAvailableRateUpdateProcessor(BaseEventProcessor):
    """Processor for bestAvailableRateUpdate events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "bestAvailableRateUpdate"
        print("--------------------------------")
        print("in BestAvailableRateUpdateProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        
        return {
            "hotel": hotel_df,
        }

class InventoryBatchProcessor(BaseEventProcessor):
    """Processor for inventoryBatch events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "inventoryBatch"
        print("--------------------------------")
        print("in InventoryBatchProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        
        return {
            "hotel": hotel_df,
        }

class InventoryUpdateProcessor(BaseEventProcessor):
    """Processor for inventoryUpdate events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "inventoryUpdate"
        print("--------------------------------")
        print("in InventoryBatchProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        
        return {
            "hotel": hotel_df,
        }

class housekeepingStatusProcessor(BaseEventProcessor):
    """Processor for housekeepingStatus events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "housekeepingStatus"
        print("--------------------------------")
        print("in housekeepingStatusProcessor")
        print(f"event type: {event_type}")
        
        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df, get_table_callable("hotel"))
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df, get_table_callable("confirmationNumber"))
        

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
        }

class FolioGuestCreateIncrementalUpdateProcessor(BaseEventProcessor):
    """Processor for folioGuestCreate and folioIncrementalUpdate events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        print("--------------------------------")
        print("in FolioGuestCreateIncrementalUpdateProcessor")
    pass

class FolioGroupCreateIncrementalUpdateProcessor(BaseEventProcessor):
    """Processor for folioGroupCreate and folioGroupIncrementalUpdate events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        print("--------------------------------")
        print("in FolioGroupCreateIncrementalUpdateProcessor")
    pass

class FolioHouseAccountCreateIncrementalUpdateProcessor(BaseEventProcessor):
    """Processor for folioHouseAccountCreate and folioHouseAccountIncrementalUpdate events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        print("--------------------------------")
        print("in FolioHouseAccountCreateIncrementalUpdateProcessor")
    pass

class InvoiceCreateIncrementalUpdateProcessor(BaseEventProcessor):
    """Processor for invoiceCreate and invoiceIncrementalUpdate events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        print("--------------------------------")
        print("in InvoiceCreateIncrementalUpdateProcessor")
    pass