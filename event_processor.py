from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, Callable, Any, Optional, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
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
from json_utils import extract_json_value
from functools import reduce
from decimal import Decimal, InvalidOperation
from datetime import datetime, date
from relationship import get_table_relationships, table_exists_in_schema


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
                
                # Handle malformed format like "2025-12-29 08:07:192Z"
                # where milliseconds are missing the decimal point
                if value.endswith('Z') and len(value) > 20:
                    # Check if it matches pattern YYYY-MM-DD HH:MM:SSXXXZ
                    import re
                    malformed_pattern = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(\d+)Z$'
                    match = re.match(malformed_pattern, value)
                    if match:
                        # Extract base timestamp and milliseconds
                        base_ts = match.group(1)
                        millis = match.group(2)
                        # Reconstruct with proper decimal point
                        corrected_value = f"{base_ts}.{millis}"
                        try:
                            return datetime.strptime(corrected_value, '%Y-%m-%d %H:%M:%S.%f')
                        except ValueError:
                            pass
                
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


def safe_batch_extract(data: Any, field_mapping: Dict[str, str], udf_schema: StructType) -> Dict[str, Any]:
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
        if not path:
            results[field.name] = None
            continue
            
        # Use your updated utility
        raw_value = extract_json_value(data, path)
        
        # Safe type conversion
        converted_value = safe_type_conversion(raw_value, field.dataType)
        
        # JVM Null handling for Strings
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
        self.STRING_NULL_MARKER = "___NULL___"
        
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

        if(df is None or df.rdd.isEmpty()):
            print(f"table name: {table_name}")
            print("Input DF is empty or None, returning empty DFs.")
            empty_df = self.spark.createDataFrame([], full_schema)
            return {"succeeded": empty_df, "failed": empty_df}
        
        df_size_before = df.count()

        # STEP 1: Replace String NULL markers with actual None
        for field in full_schema.fields:
            if isinstance(field.dataType, StringType) and field.name in df.columns:
                # print(f"Replacing NULL markers in column: {field.name}")
                df = df.withColumn(
                    field.name,
                    F.when(
                        F.col(field.name) == self.STRING_NULL_MARKER,
                        F.lit(None)
                    ).otherwise(F.col(field.name))
                )

        # STEP 2: Map Metadata
        if "source_id" not in df.columns:
            df = df.withColumn("source_id", F.col("id"))
        
        # If table is reservationStatus, use statusTimestamp as received_at
        if table_name == "reservationStatus":
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
        print("final_df")
        
        # STEP 5: VALIDATION - Split based on nullability
        # This will move records with NULL in non-nullable columns to failed_df
        df_size_after = final_df.count()
        print("Finalized DF with correct schema. Before: {}, After: {}".format(df_size_before, df_size_after))
        valid_df, failed_df = self.validate_schema_nullability(final_df, full_schema)
        
        valid_df_size = valid_df.count()
        failed_df_size = failed_df.count()
        print("DF after validating nullability. Valid DF size: {}, Failed DF size: {}".format(valid_df_size, failed_df_size))

        # STEP 6: AUDIT - Only audit valid records
        audited_valid_df = self.audit_processor(table_name, valid_df)

        return {
            "succeeded": audited_valid_df,
            "failed": failed_df,
            "lookup": valid_df
        }
    
    def process_dataframe(self, df: DataFrame, field_mapping: Any, table_name: str, parent_df_list: list[Dict[str, DataFrame]] = None) -> Dict[str, DataFrame]:
        """
        Unified entry point for extracting table data.
        """
        if not field_mapping:
            print(f"CRITICAL: No mapping found for table {table_name}. Returning empty DF.")
            empty_df = self.spark.createDataFrame([], get_table_schema(table_name))
            return {"succeeded": empty_df, "failed": empty_df}

        # Handling list of mappings (common in CheckOut events)
        print("--------------------------------")
        print("in process_dataframe")
        print("handling list of mappings")
        if isinstance(field_mapping, list):
            merged_mapping = {}
            for m in field_mapping:
                if isinstance(m, dict): merged_mapping.update(m)
            field_mapping = merged_mapping

        # Determine if this table contains array-based rows by checking for [] in paths
        # or by checking if the table is known to be an array table
        is_array_table = any("[]" in str(path) for path in field_mapping.values())
        print("determining if table is array table")
        print(f"table: {table_name}")

        print("extracting data from table using array or single approach")
        if is_array_table:
            processed_df = self._extract_array(df, field_mapping, table_name)
        else:
            processed_df = self._extract_single(df, field_mapping, table_name)
        
        if(processed_df is not None):
            print("processed_df after extracting data from table using array or single approach")
        else:
            print("processed_df after extracting data from table using array or single approach is None")
            return {"succeeded": None, "failed": None}
        
        processed_df = processed_df.withColumn("source_id", F.col("id"))
        
        if(table_exists_in_schema(table_name)):
            print(f"table {table_name} has parent tables")
            relationships = get_table_relationships(table_name)
            for parent_table, relation_info in relationships.items():
                
                print(f"pranet table: {parent_table}")
                local_column = relation_info["local_column"]
                print(f"child table field: {local_column}")
                parent_column = relation_info["parent_column"]
                print(f"parent table field: {parent_column}")

                parent_df = None
                for d in parent_df_list:
                    if parent_table in d:
                        parent_df = d[parent_table]
                        break

                if(parent_df is None):
                    print(f"parent table {parent_table} not found for table {table_name}")
                    continue
                
                print("parent_df dataframe")
                parent_df.show(20, truncate=False)

                parent_lookup = parent_df.select(
                    F.col(parent_column).alias("parent_pk_val"), 
                    F.col("source_id").alias("p_source_id")
                )
                print("parent_lookup dataframe")
                parent_lookup.show(20, truncate=False)

                processed_df = processed_df.join(
                    parent_lookup,
                    processed_df["source_id"] == parent_lookup["p_source_id"],
                    "left"
                ).withColumn(local_column, F.col("parent_pk_val")).drop("parent_pk_val", "p_source_id")
        
        return self.finalize_and_audit(processed_df, table_name, get_table_schema(table_name))

    def _extract_single(self, df: DataFrame, field_mapping: Dict[str, str], table_name: str) -> DataFrame:
        """
        Extracts 1 row of data for every 1 event row.
        """
        print("--------------------------------")
        print("in _extract_single")
        full_schema = get_table_schema(table_name)
        udf_fields = [field for field in full_schema.fields if field.name in field_mapping]
        udf_schema = StructType(udf_fields)

        @F.udf(returnType=udf_schema)
        def single_batch_udf(raw_str):
            if not raw_str: 
                return None
            try:
                data = json.loads(raw_str) if isinstance(raw_str, str) else raw_str
                return safe_batch_extract(data, field_mapping, udf_schema)
            except Exception as e:
                # Log the error for debugging but return None to avoid crashing
                print(f"Error in batch_extract_udf: {str(e)}")
                return None
        
        df = df.withColumn("extracted_data", single_batch_udf(F.col("raw")))
        for field in udf_schema.fields:
            df = df.withColumn(field.name, F.col(f"extracted_data.{field.name}"))
        
        return df.drop("extracted_data")

    def _extract_array(self, df: DataFrame, field_mapping: Dict[str, str], table_name: str) -> DataFrame:
        """
        Handles 1 event -> N rows (e.g. Confirmation Numbers).
        Uses your [] notation to zip parallel lists into rows.
        """
        full_schema = get_table_schema(table_name)
        udf_fields = [field for field in full_schema.fields if field.name in field_mapping]
        udf_schema = StructType(udf_fields)

        @F.udf(returnType=ArrayType(udf_schema))
        def array_batch_udf(raw_str):
            if not raw_str: return []
            try:
                data = json.loads(raw_str) if isinstance(raw_str, str) else raw_str
                
                # 1. Extract all fields. Since paths have [], these will be lists.
                # Example: field_results['confirmationNumber'] = ['101', '795']
                field_results = {}
                max_len = 0
                
                for field_name, path in field_mapping.items():
                    vals = extract_json_value(data, path)
                    # If it's a single value, wrap in list for zipping
                    if not isinstance(vals, list):
                        vals = [vals] if vals is not None else []
                    
                    field_results[field_name] = vals
                    max_len = max(max_len, len(vals))

                # 2. Zip parallel lists into Row structures
                rows = []
                for i in range(max_len):
                    row_data = {}
                    for field in udf_schema.fields:
                        # Get value at index i, or None if list is shorter
                        val_list = field_results.get(field.name, [])
                        raw_val = val_list[i] if i < len(val_list) else None
                        
                        # Type conversion
                        converted = safe_type_conversion(raw_val, field.dataType)
                        
                        # JVM Marker
                        if isinstance(field.dataType, StringType) and converted is None:
                            converted = "___NULL___"
                        row_data[field.name] = converted
                    rows.append(row_data)
                return rows
            except Exception as e:
                print(f"Error in array_batch_udf: {e}")
                return []

        # Explode the list of structs into multiple rows
        df = df.withColumn("extracted_array", array_batch_udf(F.col("raw")))
        df = df.withColumn("exploded_row", F.explode_outer(F.col("extracted_array")))
        
        # Flatten the struct columns
        for field in udf_schema.fields:
            df = df.withColumn(field.name, F.col(f"exploded_row.{field.name}"))
            
        return df.drop("extracted_array", "exploded_row")

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
    
    def hotel_processor(self, event_type: str, event_df: DataFrame) -> Dict[str, DataFrame]:
        print("--------------------------------")
        print("in hotel_processor")

        print("getting hotel field mappings")
        hotel_fields = get_event_table_mappings(event_type, "hotel")
        # print(f"Hotel fields: {hotel_fields}")
        
        # 1. Get hotel dataframe with audited unique records
        print("processing hotel dataframe")
        extracted_hotel_df = self.process_dataframe(event_df, hotel_fields, table_name="hotel")
        print("hotel dataframe processed")
        
        # 2. Apply Property Name Abbreviation Logic
        print("applying property name abbreviation logic")
        def abbreviate_name(df):
            if "propertyName" not in df.columns: return df
            return df.withColumn("propertyName_abbrev", 
                F.when(F.col("propertyName").rlike("^[A-Za-z]+ [A-Za-z]+ "),
                F.concat(F.upper(F.substring(F.regexp_extract(F.col("propertyName"), r"^([A-Za-z]+)", 1), 1, 1)),
                         F.upper(F.substring(F.regexp_extract(F.col("propertyName"), r"^[A-Za-z]+ ([A-Za-z]+)", 1), 1, 1)),
                         F.lit(" "), F.regexp_extract(F.col("propertyName"), r"^[A-Za-z]+ [A-Za-z]+ (.+)", 1))
                ).otherwise(F.col("propertyName")))
        
        print("property name abbreviation logic applied")
        return {
            "succeeded": abbreviate_name(extracted_hotel_df["succeeded"]),
            "failed": extracted_hotel_df["failed"],
            "lookup": extracted_hotel_df["lookup"]
        }
    
    def generic_table_processor(self, event_type: str, table_name: str, event_df: DataFrame, parent_df: list[Dict[str, DataFrame]] = None) -> Dict[str, DataFrame]:
        print("--------------------------------")
        print("in generic_table_processor")
        print(f"event type: {event_type}")
        print(f"table name: {table_name}")

        table_fields = get_event_table_mappings(event_type, table_name)
        return self.process_dataframe(event_df, table_fields, table_name, parent_df)
    
    def reservation_checkout_confirmation_number_processor(self, event_type: str, event_df: DataFrame) -> Dict[str, DataFrame]:
        # 1. Get the mappings
        mappings = get_event_table_mappings(event_type, "confirmationNumber")
        
        # 2. Extract DataFrames into a list
        # Guard: Ensure mappings is a list
        if not isinstance(mappings, list):
            mappings = [mappings] if mappings else []

        s_list = []
        f_list = []
        l_list = []
        for mapping in mappings:
            if mapping: # Guard against None entries in list
                res = self.process_dataframe(event_df, mapping, table_name="confirmationNumber")
                s_list.append(res["succeeded"])
                f_list.append(res["failed"])
                l_list.append(res["lookup"])



        # 3. Handle empty list case (to avoid errors)
        empty = self.spark.createDataFrame([], get_table_schema("confirmationNumber"))

        return {
            "succeeded": reduce(lambda d1, d2: d1.unionByName(d2), s_list) if s_list else empty,
            "failed": reduce(lambda d1, d2: d1.unionByName(d2), f_list) if f_list else empty,
            "lookup": reduce(lambda d1, d2: d1.unionByName(d2), l_list) if l_list else empty,
        }
    
    def link_table_processor(
        self, 
        final_table_name: str,
        table_1: str,
        df_1: DataFrame,
        table_2: str,
        df_2: DataFrame,
    ) -> Dict[str, DataFrame]:
        """
        Links two dataframes based on source_id and creates a mapping between their ID columns.
        
        Args:
            final_table_name: Name of the final table to link the two tables
            table_1: Name of the first table to link
            df_1: First dataframe to link with source_id and table_1_id
            table_2: Name of the second table to link
            df_2: Second dataframe to link with source_id and table_2_id
        
        Returns:
            DataFrame with the linked tables with source_id, table_1_id, and table_2_id
        """

        print("--------------------------------")
        print(f"processing link table {final_table_name} with tables {table_1} and {table_2}")

        if(df_1 is None or df_2 is None):
            print(f"Warning: df_1 or df_2 is None for table {final_table_name} in link_table_processor.")
            return None
        
        relationship_info = get_table_relationships(final_table_name)
        
        table_1_info = relationship_info.get(table_1)
        print(f"table_1_info: {table_1_info}")
        final_df_1_id = table_1_info.get("local_column")
        table_1_id = table_1_info.get("parent_column")
        
        table_2_info = relationship_info.get(table_2)
        print(f"table_2_info: {table_2_info}")
        final_df_2_id = table_2_info.get("local_column")
        table_2_id = table_2_info.get("parent_column")

        # Select only source_id and the ID column from each dataframe
        print("selecting source_id, table_1_id from df_1")

        print(f"{table_1_id}, {table_2_id}")

        d1 = df_1.select(F.col("source_id").alias("s_id_1"), F.col(table_1_id).alias(final_df_1_id))
        d2 = df_2.select(F.col("source_id").alias("s_id_2"), F.col("received_at"), F.col(table_2_id).alias(final_df_2_id))

        d1.show(20, truncate=False)
        d2.show(20, truncate=False)

        linked_df = d1.join(
            d2,
            d1["s_id_1"] == d2["s_id_2"],
            how="inner"
        )
        # df_1_subset = df_1[['source_id', table_1_id]]
        # df_1_subset.show(20, truncate=False)
        # print("selecting source_id, table_2_id, received_at from df_2")
        # df_2_subset = df_2[['source_id', table_2_id, 'received_at']]
        # df_2_subset.show(20, truncate=False)
        # # Perform inner join on source_id to link matching records
        # print("performing inner join on source_id to link records originating from the same payload")
        # result_df = df_1_subset.merge(
        #     df_2_subset, 
        #     on='source_id', 
        #     how='inner'
        # )
        # result_df.show(20, truncate=False)
        # # Select only the ID columns and rename them
        # print("renaming table_1_id and table_2_id to final_df_1_id and final_df_2_id and selecting source_id, received_at")
        # final_df = result_df[[table_1_id, table_2_id, 'received_at', 'source_id']].rename(
        #     columns={
        #         table_1_id: final_df_1_id,
        #         table_2_id: final_df_2_id
        #     }
        # )
        # print("final_df")
        # final_df.show(20, truncate=False)
        # print("--------------------------------")
        # return final_df
        linked_df = linked_df.select(
            F.col("s_id_1").alias("source_id"),
            F.col("received_at"),
            F.col(final_df_1_id),
            F.col(final_df_2_id)
        )

        linked_df = self.generate_record_hash(
            linked_df, 
            business_fields=[final_df_1_id, final_df_2_id], 
            hash_column_name="link_hash"
        )
        linked_df = linked_df.withColumn("id", to_uuid_v5(F.col("link_hash")))
        linked_df = linked_df.drop("link_hash")

        return {
            "succeeded": linked_df,
            "failed": None
        }
    
    def reservation_group_processor(self, event_type: str, table_name: str, event_df: DataFrame, parent_table_name: str, parent_df: DataFrame) -> Dict[str, DataFrame]:

        # 1. Get the mappings
        mappings = get_event_table_mappings(event_type, table_name)
        
        # 2. Extract DataFrames into a list
        # Guard: Ensure mappings is a list
        if not isinstance(mappings, list):
            mappings = [mappings] if mappings else []


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
        print(f"Processing event type: {event_type}")
        result_dict = processor_instance.process(event_df)
        
        # Collect the processed dataframe, perform left anti join with the existing processed dataframe to get unique records
        
        # Guard: ensure result_dict is iterable
        if result_dict is None or not isinstance(result_dict, dict):
            print(f"WARNING: Processor for {event_type} returned None or non-dict. Skipping.")
            continue

        for table_name, split_dfs in result_dict.items():
            # result_dict[table_name] is {"succeeded": df, "failed": df}
            succeeded_df = split_dfs.get("succeeded")
            failed_df = split_dfs.get("failed")
            
            if succeeded_df is not None:
                if table_name not in success_accumulator:
                    success_accumulator[table_name] = succeeded_df
                else:
                    success_accumulator[table_name] = success_accumulator[table_name].unionByName(succeeded_df)
            
            if failed_df is not None:
                if table_name not in failed_accumulator:
                    failed_accumulator[table_name] = failed_df
                else:
                    failed_accumulator[table_name] = failed_accumulator[table_name].unionByName(failed_df)
    
    # FINAL DEDUPLICATION: Pick one record per ID across all unions
    # Since these are hotel records, we usually want the LATEST one 
    # (the one with the most recent received_at)

    final_success = {}
    for table_name, table_df in success_accumulator.items():
        # Sort by received_at descending so the newest record is on top, then drop duplicates
        # Only process if we actually have rows
        # Filter out null IDs and received_at BEFORE attempting count

        cols = table_df.columns
        has_id = "id" in cols
        has_received_at = "received_at" in cols

        conditions = []
        if has_id:
            conditions.append(F.col("id").isNotNull())
        if has_received_at:
            conditions.append(F.col("received_at").isNotNull())

        safe_df = table_df

        if conditions:
            # Combine conditions with AND
            combined_cond = reduce(lambda a, b: a & b, conditions)
            safe_df = table_df.filter(combined_cond)
        
        try:
            # Only perform deduplication if both columns are present
            if has_id and has_received_at:
                if safe_df.limit(1).count() > 0: 
                    final_success[table_name] = safe_df.orderBy(F.col("received_at").desc()).dropDuplicates(["id"])
            elif has_received_at:
                # If no ID, just sort by latest for visibility
                if safe_df.limit(1).count() > 0:
                    final_success[table_name] = safe_df.orderBy(F.col("received_at").desc())
            else:
                final_success[table_name] = safe_df
        except Exception as e:
            print(f"WARNING: Error processing {table_name} in success_accumulator: {e}")
            continue

        # Use try-except to handle any remaining NPE issues gracefully
    
    final_failed = {}
    for table_name, table_df in failed_accumulator.items():
        # Sort by received_at descending so the newest record is on top, then drop duplicates
        # Filter out null IDs and received_at BEFORE attempting count
        cols = table_df.columns
        has_id = "id" in cols
        has_ts = "received_at" in cols

        safe_df = table_df
        if has_id and has_ts:
            if safe_df.limit(1).count() > 0:
                safe_df = table_df.filter(F.col("id").isNotNull() & F.col("received_at").isNotNull())
                final_failed[table_name] = safe_df.orderBy(F.col("received_at").desc()).dropDuplicates(["id"])
        else:
            final_failed[table_name] = table_df

    return final_success, final_failed

class ReservationCreateModifyProcessor(BaseEventProcessor):
    """Processor for reservationCreate and reservationModify events."""

    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        event_type = "reservationCreate"
        print("--------------------------------")
        print("in ReservationCreateModifyProcessor")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df)
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df)
        reservation_status_df = self.generic_table_processor(event_type, "reservationStatus", event_df)
        customer_df = self.generic_table_processor(event_type, "customer", event_df)
        contact_purpose_df = self.generic_table_processor(event_type, "contactPurpose", event_df)
        contact_df = self.generic_table_processor(event_type, "contact", event_df)
        loyalty_reward_membership_df = self.generic_table_processor(event_type, "loyaltyRewardsMembership", event_df)
        reservation_item_df = self.generic_table_processor(event_type, "reservationItem", event_df)
        reserved_inventory_df = self.generic_table_processor(event_type, "reservedInventory", event_df)
        reserved_rate_df = self.generic_table_processor(event_type, "reservedRate", event_df)
        reserved_room_df = self.generic_table_processor(event_type, "reservedRoom", event_df)
        special_request_df = self.generic_table_processor(event_type, "specialRequest", event_df)
        guarantee_df = self.generic_table_processor(event_type, "guarantee", event_df)
        credit_card_df = self.generic_table_processor(event_type, "creditCard", event_df)
        payment_method_df = self.generic_table_processor(event_type, "paymentMethod", event_df)
        travel_agent_df = self.generic_table_processor(event_type, "travelAgent", event_df)
        company_df = self.generic_table_processor(event_type, "company", event_df)
        linked_reservation_df = self.generic_table_processor(event_type, "linkedReservation", event_df)

        reservation_parent_df = [{"hotel": hotel_df["lookup"]}, {"reservedRoom": reserved_room_df["lookup"]}, {"travelAgent": travel_agent_df["lookup"]}, {"company": company_df["lookup"]}]
        reservation_df = self.generic_table_processor(event_type, "reservation", event_df, reservation_parent_df)

        reservation_confirmation_number_link_df = self.link_table_processor("reservationConfirmationNumberLink", "reservation", reservation_df["lookup"], "confirmationNumber", confirmation_number_df["lookup"])
        reservation_status_link_df = self.link_table_processor("reservationStatusLink", "reservation", reservation_df["lookup"], "reservationStatus", reservation_status_df["lookup"])
        reservation_customer_link_df = self.link_table_processor("reservationCustomerLink", "reservation", reservation_df["lookup"], "customer", customer_df["lookup"])
        customer_loyalty_rewards_membership_link_df = self.link_table_processor("customerLoyaltyRewardsMembershipLink", "customer", customer_df["lookup"], "loyaltyRewardsMembership", loyalty_reward_membership_df["lookup"])
        customer_contact_link_df = self.link_table_processor("customerContactLink", "customer", customer_df["lookup"], "contact", contact_df["lookup"])
        contact_contact_purpose_link_df = self.link_table_processor("contactContactPurposeLink", "contact", contact_df["lookup"], "contactPurpose", contact_purpose_df["lookup"])
        reservation_item_link_df = self.link_table_processor("reservationItemLink", "reservation", reservation_df["lookup"], "reservationItem", reservation_item_df["lookup"])
        reservation_item_reserved_inventory_link_df = self.link_table_processor("reservationItemReservedInventoryLink", "reservationItem", reservation_item_df["lookup"], "reservedInventory", reserved_inventory_df["lookup"])
        reservation_item_reserved_rate_link_df = self.link_table_processor("reservationItemReservedRateLink", "reservationItem", reservation_item_df["lookup"], "reservedRate", reserved_rate_df["lookup"])
        reservation_item_special_request_link_df = self.link_table_processor("reservationItemSpecialRequestLink", "reservationItem", reservation_item_df["lookup"], "specialRequest", special_request_df["lookup"])
        reservation_guarantee_link_df = self.link_table_processor("reservationGuaranteeLink", "reservation", reservation_df["lookup"], "guarantee", guarantee_df["lookup"])
        reservation_payment_method_link_df = self.link_table_processor("reservationPaymentMethodLink", "reservation", reservation_df["lookup"], "paymentMethod", payment_method_df["lookup"])
        payment_method_credit_card_link_df = self.link_table_processor("paymentMethodCreditCardLink", "paymentMethod", payment_method_df["lookup"], "creditCard", credit_card_df["lookup"])
        reservation_linked_reservation_link_df = self.link_table_processor("reservationLinkedReservationLink", "reservation", reservation_df["lookup"], "linkedReservation", linked_reservation_df["lookup"])

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "reservationStatus": reservation_status_df,
            "customer": customer_df,
            "contactPurpose": contact_purpose_df,
            "contact": contact_df,
            "loyaltyRewardsMembership": loyalty_reward_membership_df,
            "reservationItem": reservation_item_df,
            "reservedInventory": reserved_inventory_df,
            "reservedRate": reserved_rate_df,
            "reservedRoom": reserved_room_df,
            "specialRequest": special_request_df,
            "guarantee": guarantee_df,
            "creditCard": credit_card_df,
            "paymentMethod": payment_method_df,
            "travelAgent": travel_agent_df,
            "company": company_df,
            "linkedReservation": linked_reservation_df,
            
            "reservation": reservation_df,

            "reservationConfirmationNumberLink": reservation_confirmation_number_link_df,
            "reservationStatusLink": reservation_status_link_df,
            "reservationCustomerLink": reservation_customer_link_df,
            "customerLoyaltyRewardsMembershipLink": customer_loyalty_rewards_membership_link_df,
            "customerContactLink": customer_contact_link_df,
            "contactContactPurposeLink": contact_contact_purpose_link_df,
            "reservationItemLink": reservation_item_link_df,
            "reservationItemReservedInventoryLink": reservation_item_reserved_inventory_link_df,
            "reservationItemReservedRateLink": reservation_item_reserved_rate_link_df,
            "reservationItemSpecialRequestLink": reservation_item_special_request_link_df,
            "reservationGuaranteeLink": reservation_guarantee_link_df,
            "reservationPaymentMethodLink": reservation_payment_method_link_df,
            "paymentMethodCreditCardLink": payment_method_credit_card_link_df,
            "reservationLinkedReservationLink": reservation_linked_reservation_link_df,
        }

class ReservationCancelProcessor(BaseEventProcessor):
    """Processor for reservationCancel events."""

    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:

        event_type = "reservationCancel"
        print("--------------------------------")
        print("in ReservationCancelProcessor")
        print(f"event type: {event_type}")
        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df)
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df)
        customer_df = self.generic_table_processor(event_type, "customer", event_df)
        contact_purpose_df = self.generic_table_processor(event_type, "contactPurpose", event_df)
        contact_df = self.generic_table_processor(event_type, "contact", event_df)
        company_df = self.generic_table_processor(event_type, "company", event_df)
        reserved_room_df = self.generic_table_processor(event_type, "reservedRoom", event_df)

        # phase 2 tables
        reservation_parent_df = [{"hotel": hotel_df["lookup"]}, {"reservedRoom": reserved_room_df["lookup"]}, {"company": company_df["lookup"]}]
        reservation_df = self.generic_table_processor(event_type, "reservation", event_df, reservation_parent_df)

        reservation_confirmation_number_link_df = self.link_table_processor("reservationConfirmationNumberLink", "reservation", reservation_df["lookup"], "confirmationNumber", confirmation_number_df["lookup"])
        reservation_customer_link_df = self.link_table_processor("reservationCustomerLink", "reservation", reservation_df["lookup"], "customer", customer_df["lookup"])
        customer_contact_link_df = self.link_table_processor("customerContactLink", "customer", customer_df["lookup"], "contact", contact_df["lookup"])
        contact_contact_purpose_link_df = self.link_table_processor("contactContactPurposeLink", "contact", contact_df["lookup"], "contactPurpose", contact_purpose_df["lookup"])

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "customer": customer_df,
            "contactPurpose": contact_purpose_df,
            "contact": contact_df,
            "company": company_df,
            "reservedRoom": reserved_room_df,
            
            "reservation": reservation_df,

            "reservationConfirmationNumberLink": reservation_confirmation_number_link_df,
            "reservationCustomerLink": reservation_customer_link_df,
            "customerContactLink": customer_contact_link_df,
            "contactContactPurposeLink": contact_contact_purpose_link_df,
        }

class ReservationCheckInProcessor(BaseEventProcessor):
    """Processor for reservationCheckIn events."""
    
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
    
        event_type = "reservationCheckIn"
        print("--------------------------------")
        print("in ReservationCheckInProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df)
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df)
        reservation_status_df = self.generic_table_processor(event_type, "reservationStatus", event_df)
        customer_df = self.generic_table_processor(event_type, "customer", event_df)
        contact_purpose_df = self.generic_table_processor(event_type, "contactPurpose", event_df)
        contact_df = self.generic_table_processor(event_type, "contact", event_df)
        company_df = self.generic_table_processor(event_type, "company", event_df)
        reserved_room_df = self.generic_table_processor(event_type, "reservedRoom", event_df)
        
        reservation_parent_df = [{"hotel": hotel_df["lookup"]}, {"reservedRoom": reserved_room_df["lookup"]}, {"company": company_df["lookup"]}]
        reservation_df = self.generic_table_processor(event_type, "reservation", event_df, reservation_parent_df)

        reservation_confirmation_number_link_df = self.link_table_processor("reservationConfirmationNumberLink", "reservation", reservation_df["lookup"], "confirmationNumber", confirmation_number_df["lookup"])
        reservation_status_link_df = self.link_table_processor("reservationStatusLink", "reservation", reservation_df["lookup"], "reservationStatus", reservation_status_df["lookup"])
        reservation_customer_link_df = self.link_table_processor("reservationCustomerLink", "reservation", reservation_df["lookup"], "customer", customer_df["lookup"])
        customer_contact_link_df = self.link_table_processor("customerContactLink", "customer", customer_df["lookup"], "contact", contact_df["lookup"])
        contact_contact_purpose_link_df = self.link_table_processor("contactContactPurposeLink", "contact", contact_df["lookup"], "contactPurpose", contact_purpose_df["lookup"])
        
        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "reservationStatus": reservation_status_df,
            "customer": customer_df,
            "contactPurpose": contact_purpose_df,
            "contact": contact_df,
            "company": company_df,
            "reservedRoom": reserved_room_df,

            "reservation": reservation_df,
            
            "reservationConfirmationNumberLink": reservation_confirmation_number_link_df,
            "reservationStatusLink": reservation_status_link_df,
            "reservationCustomerLink": reservation_customer_link_df,
            "customerContactLink": customer_contact_link_df,
            "contactContactPurposeLink": contact_contact_purpose_link_df,
        }

class ReservationCheckOutProcessor(BaseEventProcessor):
    """Processor for reservationCheckOut events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:

        event_type = "reservationCheckOut"
        print("--------------------------------")
        print("in ReservationCheckOutProcessor")
        print(f"event type: {event_type}")
        
        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df)
        confirmation_number_df = self.reservation_checkout_confirmation_number_processor(event_type, event_df)
        reservation_status_df = self.generic_table_processor(event_type, "reservationStatus", event_df)
        customer_df = self.generic_table_processor(event_type, "customer", event_df)
        contact_purpose_df = self.generic_table_processor(event_type, "contactPurpose", event_df)
        contact_df = self.generic_table_processor(event_type, "contact", event_df)
        company_df = self.generic_table_processor(event_type, "company", event_df)
        reserved_room_df = self.generic_table_processor(event_type, "reservedRoom", event_df)
        # currently not importing the foliosummary table

        reservation_parent_df = [{"hotel": hotel_df["lookup"]}, {"reservedRoom": reserved_room_df["lookup"]}, {"company": company_df["lookup"]}]
        reservation_df = self.generic_table_processor(event_type, "reservation", event_df, reservation_parent_df)

        reservation_confirmation_number_link_df = self.link_table_processor("reservationConfirmationNumberLink", "reservation", reservation_df["lookup"], "confirmationNumber", confirmation_number_df["lookup"])
        reservation_status_link_df = self.link_table_processor("reservationStatusLink", "reservation", reservation_df["lookup"], "reservationStatus", reservation_status_df["lookup"])
        reservation_customer_link_df = self.link_table_processor("reservationCustomerLink", "reservation", reservation_df["lookup"], "customer", customer_df["lookup"])
        customer_contact_link_df = self.link_table_processor("customerContactLink", "customer", customer_df["lookup"], "contact", contact_df["lookup"])
        contact_contact_purpose_link_df = self.link_table_processor("contactContactPurposeLink", "contact", contact_df["lookup"], "contactPurpose", contact_purpose_df["lookup"])
        

        # # phase 2 tables
        # group_parent_df = [{"hotel": hotel_success_df}, {"company": company_success_df}]
        # group_df = self.generic_table_processor(event_type, "group", event_df, group_parent_df)

        reservation_confirmation_number_link_df = self.link_table_processor("reservationConfirmationNumberLink", "reservation", reservation_df["lookup"], "confirmationNumber", confirmation_number_df["lookup"])
        reservation_status_link_df = self.link_table_processor("reservationStatusLink", "reservation", reservation_df["lookup"], "reservationStatus", reservation_status_df["lookup"])
        reservation_customer_link_df = self.link_table_processor("reservationCustomerLink", "reservation", reservation_df["lookup"], "customer", customer_df["lookup"])
        customer_contact_link_df = self.link_table_processor("customerContactLink", "customer", customer_df["lookup"], "contact", contact_df["lookup"])
        contact_contact_purpose_link_df = self.link_table_processor("contactContactPurposeLink", "contact", contact_df["lookup"], "contactPurpose", contact_purpose_df["lookup"])
        
        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "reservationStatus": reservation_status_df,
            "customer": customer_df,
            "contactPurpose": contact_purpose_df,
            "contact": contact_df,  
            "company": company_df,
            "reservedRoom": reserved_room_df,
            
            "reservation": reservation_df,
            # "group": group_df,

            "reservationConfirmationNumberLink": reservation_confirmation_number_link_df,
            "reservationStatusLink": reservation_status_link_df,
            "reservationCustomerLink": reservation_customer_link_df,
            "customerContactLink": customer_contact_link_df,
            "contactContactPurposeLink": contact_contact_purpose_link_df,
        }

class ReservationRoomAssignedProcessor(BaseEventProcessor):
    """Processor for reservationRoomAssigned events."""

    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:

        event_type = "reservationRoomAssigned"
        print("--------------------------------")
        print("in ReservationRoomAssignedProcessor")
        print(f"event type: {event_type}")
        
        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df)
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df)
        reservation_status_df = self.generic_table_processor(event_type, "reservationStatus", event_df)
        customer_df = self.generic_table_processor(event_type, "customer", event_df)
        contact_purpose_df = self.generic_table_processor(event_type, "contactPurpose", event_df)
        contact_df = self.generic_table_processor(event_type, "contact", event_df)
        reserved_room_df = self.generic_table_processor(event_type, "reservedRoom", event_df)

        reservation_parent_df = [{"hotel": hotel_df["lookup"]}, {"reservedRoom": reserved_room_df["lookup"]}]
        reservation_df = self.generic_table_processor(event_type, "reservation", event_df, reservation_parent_df)

        reservation_confirmation_number_link_df = self.link_table_processor("reservationConfirmationNumberLink", "reservation", reservation_df["lookup"], "confirmationNumber", confirmation_number_df["lookup"])
        reservation_status_link_df = self.link_table_processor("reservationStatusLink", "reservation", reservation_df["lookup"], "reservationStatus", reservation_status_df["lookup"])
        reservation_customer_link_df = self.link_table_processor("reservationCustomerLink", "reservation", reservation_df["lookup"], "customer", customer_df["lookup"])
        customer_contact_link_df = self.link_table_processor("customerContactLink", "customer", customer_df["lookup"], "contact", contact_df["lookup"])
        contact_contact_purpose_link_df = self.link_table_processor("contactContactPurposeLink", "contact", contact_df["lookup"], "contactPurpose", contact_purpose_df["lookup"])
        

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "reservationStatus": reservation_status_df,
            "customer": customer_df,
            "contactPurpose": contact_purpose_df,
            "contact": contact_df,
            "reservedRoom": reserved_room_df,

            "reservation": reservation_df,

            "reservationConfirmationNumberLink": reservation_confirmation_number_link_df,
            "reservationStatusLink": reservation_status_link_df,
            "reservationCustomerLink": reservation_customer_link_df,
            "customerContactLink": customer_contact_link_df,
            "contactContactPurposeLink": contact_contact_purpose_link_df,
        }

class ReservationRoomChangeProcessor(BaseEventProcessor):
    """Processor for reservationRoomChange events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "reservationRoomChange"
        print("--------------------------------")
        print("in ReservationRoomChangeProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df)
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df)
        reservation_status_df = self.generic_table_processor(event_type, "reservationStatus", event_df)
        customer_df = self.generic_table_processor(event_type, "customer", event_df)
        contact_purpose_df = self.generic_table_processor(event_type, "contactPurpose", event_df)
        contact_df = self.generic_table_processor(event_type, "contact", event_df)
        reserved_room_df = self.generic_table_processor(event_type, "reservedRoom", event_df)

        reservation_parent_df = [{"hotel": hotel_df["lookup"]}, {"reservedRoom": reserved_room_df["lookup"]}]
        reservation_df = self.generic_table_processor(event_type, "reservation", event_df, reservation_parent_df)

        reservation_confirmation_number_link_df = self.link_table_processor("reservationConfirmationNumberLink", "reservation", reservation_df["lookup"], "confirmationNumber", confirmation_number_df["lookup"])
        reservation_status_link_df = self.link_table_processor("reservationStatusLink", "reservation", reservation_df["lookup"], "reservationStatus", reservation_status_df["lookup"])
        reservation_customer_link_df = self.link_table_processor("reservationCustomerLink", "reservation", reservation_df["lookup"], "customer", customer_df["lookup"])
        customer_contact_link_df = self.link_table_processor("customerContactLink", "customer", customer_df["lookup"], "contact", contact_df["lookup"])
        contact_contact_purpose_link_df = self.link_table_processor("contactContactPurposeLink", "contact", contact_df["lookup"], "contactPurpose", contact_purpose_df["lookup"])

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "reservationStatus": reservation_status_df,
            "customer": customer_df,
            "contactPurpose": contact_purpose_df,
            "contact": contact_df,
            "reservedRoom": reserved_room_df,

            "reservation": reservation_df,

            "reservationConfirmationNumberLink": reservation_confirmation_number_link_df,
            "reservationStatusLink": reservation_status_link_df,
            "reservationCustomerLink": reservation_customer_link_df,
            "customerContactLink": customer_contact_link_df,
            "contactContactPurposeLink": contact_contact_purpose_link_df,
        }
    
class ReservationECheckInProcessor(BaseEventProcessor):
    """Processor for reservationECheckIn events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "reservationECheckIn"
        print("--------------------------------")
        print("in ReservationECheckInProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df)
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df)
        reservation_status_df = self.generic_table_processor(event_type, "reservationStatus", event_df)
        reserved_room_df = self.generic_table_processor(event_type, "reservedRoom", event_df)

        reservation_parent_df = [{"hotel": hotel_df["lookup"]}, {"reservedRoom": reserved_room_df["lookup"]}]
        reservation_df = self.generic_table_processor(event_type, "reservation", event_df, reservation_parent_df)

        # not importing housekeping status from the phase 2 table currently

        reservation_confirmation_number_link_df = self.link_table_processor("reservationConfirmationNumberLink", "reservation", reservation_df["lookup"], "confirmationNumber", confirmation_number_df["lookup"])
        reservation_status_link_df = self.link_table_processor("reservationStatusLink", "reservation", reservation_df["lookup"], "reservationStatus", reservation_status_df["lookup"])

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "reservationStatus": reservation_status_df,
            "reservedRoom": reserved_room_df,

            "reservation": reservation_df,

            "reservationConfirmationNumberLink": reservation_confirmation_number_link_df,
            "reservationStatusLink": reservation_status_link_df,
        }

class ReservationEmailConfirmationProcessor(BaseEventProcessor):
    """Processor for reservationEmailConfirmation events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:

        event_type = "reservationEmailConfirmation"
        print("--------------------------------")
        print("in ReservationEmailConfirmationProcessor")
        print(f"event type: {event_type}")
        
        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df)
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df)
        customer_df = self.generic_table_processor(event_type, "customer", event_df)
        contact_purpose_df = self.generic_table_processor(event_type, "contactPurpose", event_df)
        contact_df = self.generic_table_processor(event_type, "contact", event_df)

        reservation_parent_df = [{"hotel": hotel_df["lookup"]}, {"customer": customer_df["lookup"]}]
        reservation_df = self.generic_table_processor(event_type, "reservation", event_df, reservation_parent_df)

        reservation_confirmation_number_link_df = self.link_table_processor("reservationConfirmationNumberLink", "reservation", reservation_df["lookup"], "confirmationNumber", confirmation_number_df["lookup"])
        reservation_customer_link_df = self.link_table_processor("reservationCustomerLink", "reservation", reservation_df["lookup"], "customer", customer_df["lookup"])
        customer_contact_link_df = self.link_table_processor("customerContactLink", "customer", customer_df["lookup"], "contact", contact_df["lookup"])
        contact_contact_purpose_link_df = self.link_table_processor("contactContactPurposeLink", "contact", contact_df["lookup"], "contactPurpose", contact_purpose_df["lookup"])

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "customer": customer_df,
            "contactPurpose": contact_purpose_df,
            "contact": contact_df,

            "reservation": reservation_df,

            "reservationConfirmationNumberLink": reservation_confirmation_number_link_df,
            "reservationCustomerLink": reservation_customer_link_df,
            "customerContactLink": customer_contact_link_df,
            "contactContactPurposeLink": contact_contact_purpose_link_df,
        }

class GroupCreateModifyProcessor(BaseEventProcessor):
    """Processor for groupCreate and groupModify events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "groupCreate"
        print("--------------------------------")
        print("in GroupCreateModifyProcessor")
        print(f"event type: {event_type}")
        
        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df)
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df)
        contact_purpose_df = self.generic_table_processor(event_type, "contactPurpose", event_df)
        contact_df = self.generic_table_processor(event_type, "contact", event_df)
        company_df = self.generic_table_processor(event_type, "company", event_df)
        travel_agent_df = self.generic_table_processor(event_type, "travelAgent", event_df)

        # group_parent_df = [{"hotel": hotel_success_df}, {"company": company_success_df}, {"travelAgent": travel_agent_success_df}]
        # group_df = self.generic_table_processor(event_type, "group", event_df, group_parent_df)

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "contactPurpose": contact_purpose_df,
            "contact": contact_df,
            "company": company_df,
            "travelAgent": travel_agent_df,
            # "group": group_df,
        }

class GroupCancelProcessor(BaseEventProcessor):
    """Processor for groupCancel events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "groupCancel"
        print("--------------------------------")
        print("in GroupCancelProcessor")
        print(f"event type: {event_type}")
        
        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df)
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df)
        company_df = self.generic_table_processor(event_type, "company", event_df)

        # # phase 2 tables
        # group_parent_df = [{"hotel": hotel_success_df}, {"company": company_success_df}]
        # group_df = self.generic_table_processor(event_type, "group", event_df, group_parent_df)

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "company": company_df,
            # "group": group_df,
        }

class GroupCheckOutProcessor(BaseEventProcessor):
    """Processor for groupCheckOut events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "groupCheckOut"
        print("--------------------------------")
        print("in GroupCheckOutProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df)
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df)
        company_df = self.generic_table_processor(event_type, "company", event_df)

        # # phase 2 tables
        # group_parent_df = [{"hotel": hotel_success_df}, {"company": company_success_df}]
        # group_df = self.generic_table_processor(event_type, "group", event_df, group_parent_df)

        return {
            "hotel": hotel_df,
            "confirmationNumber": confirmation_number_df,
            "company": company_df,
            # "group": group_df,
        }

class AppliedRateUpdateProcessor(BaseEventProcessor):
    """Processor for appliedRateUpdate events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "appliedRateUpdate"
        print("--------------------------------")
        print("in AppliedRateUpdateProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df)

        #phase 2 tables
        # rate_df = self.rate_phase2_processor(event_type, "rate", event_df, hotel_df, "hotel_id")
        
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
        hotel_df = self.hotel_processor(event_type, event_df)

        #phase 2 tables
        # rate_df = self.rate_phase2_processor(event_type, "rate", event_df, hotel_df, "hotel_id")
        
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
        hotel_df = self.hotel_processor(event_type, event_df)

        #phase 2 tables
        # rate_df = self.rate_phase2_processor(event_type, "rate", event_df, hotel_df, "hotel_id")
        
        return {
            "hotel": hotel_df,
            # "rate": rate_df,
        }

class BestAvailableRateUpdateProcessor(BaseEventProcessor):
    """Processor for bestAvailableRateUpdate events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "bestAvailableRateUpdate"
        print("--------------------------------")
        print("in BestAvailableRateUpdateProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df)

        #phase 2 tables
        # rate_df = self.rate_phase2_processor(event_type, "rate", event_df, hotel_df, "hotel_id")

        return {
            "hotel": hotel_df,
            # "rate": rate_df,
        }

class InventoryBatchProcessor(BaseEventProcessor):
    """Processor for inventoryBatch events."""
    def process(self, event_df: DataFrame) -> dict[str, DataFrame]:
        
        event_type = "inventoryBatch"
        print("--------------------------------")
        print("in InventoryBatchProcessor")
        print(f"event type: {event_type}")

        # phase 1 tables
        hotel_df = self.hotel_processor(event_type, event_df)
        
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
        hotel_df = self.hotel_processor(event_type, event_df)
        
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
        hotel_df = self.hotel_processor(event_type, event_df)
        confirmation_number_df = self.generic_table_processor(event_type, "confirmationNumber", event_df)
        

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