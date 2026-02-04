import boto3
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, DoubleType

class SchemaDefinitions:
    """Centralized schema definitions for data processing."""
    
    @staticmethod
    def get_reservation_schema() -> StructType:
        """
        Define minimal schema - only import 'id' and 'raw' columns.
        This significantly improves performance by reducing data transfer.
        """
        return StructType([
            StructField("id", LongType(), True),
            StructField("datetime_received", TimestampType(), True),
            StructField("event_name", StringType(), True),
            StructField("event_version", StringType(), True),
            StructField("user_agent", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("raw", StringType(), True),
            StructField("processed", LongType(), True),
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("elapsed_sec", DoubleType(), True)
        ])


class IncrementalJobConfig:
    """Configuration constants for the incremental job."""
    BUCKET_NAME = "noble-raw-useast1-183171473439-prod"
    STATE_FILE_KEY = "last_raw_processed.json"
    BASE_PREFIX = "source=sky-touch-raw/schema=reservation/"
    MAX_CHUNK_ROWS = 500


class S3StateManager:
    """Manages state persistence in S3."""
    
    def __init__(self, bucket_name: str, state_file_key: str):
        self.bucket_name = bucket_name
        self.state_file_key = state_file_key
        self.s3_client = boto3.client('s3')
    
    def get_state(self) -> Dict:
        """Retrieve the current state from S3."""
        obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.state_file_key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    
    def update_state(self, new_state: Dict) -> None:
        """Update the state in S3."""
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.state_file_key,
            Body=json.dumps(new_state)
        )


class FileParser:
    """Utilities for parsing file names and extracting metadata."""
    
    @staticmethod
    def parse_ids_from_filename(filename: str) -> Tuple[Optional[int], Optional[int]]:
        """Extracts (start_id, end_id) from 'raw_data_653456-653548.csv'"""
        match = re.search(r'raw_data_(\d+)-(\d+)\.csv', filename)
        if match:
            return int(match.group(1)), int(match.group(2))
        return None, None


class PartitionManager:
    """Manages partition generation and navigation."""
    
    @staticmethod
    def get_next_partitions(
        year: str,
        month: str,
        day: str,
        current_date: datetime
    ) -> List[Dict[str, str]]:
        """
        Generate next partition paths (year/month/day) starting from the given date
        up to current_date.
        """
        partitions = []
        date = datetime(int(year), int(month), int(day))
        
        while date <= current_date:
            partitions.append({
                'year': str(date.year),
                'month': str(date.month).zfill(2),
                'day': str(date.day).zfill(2)
            })
            date += timedelta(days=1)
        
        return partitions


class S3FileScanner:
    """Scans S3 for files across partitions."""
    
    def __init__(self, bucket_name: str, base_prefix: str):
        self.bucket_name = bucket_name
        self.base_prefix = base_prefix
        self.s3_client = boto3.client('s3')
    
    def scan_partitions(self, partitions: List[Dict[str, str]]) -> List[Dict]:
        """
        Scan through partitions and collect all CSV files with their metadata.
        
        Returns:
            List of file objects with keys: 'key', 'start', 'end', 'year', 'month', 'day'
        """
        all_objects = []
        
        for partition in partitions:
            partition_prefix = (
                f"{self.base_prefix}year={partition['year']}/"
                f"month={partition['month']}/day={partition['day']}/"
            )
            
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=partition_prefix):
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('.csv'):
                        start, end = FileParser.parse_ids_from_filename(obj['Key'])
                        if start is not None:
                            all_objects.append({
                                'key': obj['Key'],
                                'start': start,
                                'end': end,
                                'year': partition['year'],
                                'month': partition['month'],
                                'day': partition['day']
                            })
        
        return all_objects


class FileSelector:
    """Selects target files based on state and chunk size requirements."""
    
    @staticmethod
    def get_target_files_numerically(
        state: Dict,
        current_date: datetime,
        bucket_name: str,
        base_prefix: str,
        max_chunk_rows: int
    ) -> List[Dict]:
        """
        Get target files to process based on current state and max chunk size.
        
        Returns:
            List of file objects sorted numerically by start_id
        """
        last_id = int(state['last_import_id'])
        
        # Get list of partitions to check
        partitions = PartitionManager.get_next_partitions(
            state['last_year'],
            state['last_month'],
            state['last_day'],
            current_date
        )
        
        # Scan partitions for files
        scanner = S3FileScanner(bucket_name, base_prefix)
        all_objects = scanner.scan_partitions(partitions)
        
        # Sort files NUMERICALLY by the start_id
        sorted_files = sorted(all_objects, key=lambda x: x['start'])
        
        # Filter to only files that contain data AFTER our last_id
        eligible_files = [f for f in sorted_files if f['end'] > last_id]
        
        # Determine how many files we actually need to hit MAX_CHUNK_ROWS
        target_files = []
        accumulated_potential = 0
        for f in eligible_files:
            target_files.append(f)
            # Calculate how many new rows this file contributes
            start_count = max(f['start'], last_id + 1)
            rows_in_file = f['end'] - start_count + 1
            
            accumulated_potential += rows_in_file
            if accumulated_potential >= max_chunk_rows:
                break
        
        return target_files


class DataProcessor:
    """Handles Spark data processing operations."""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
    
    def read_and_filter(
        self,
        bucket_name: str,
        files_to_read: List[Dict],
        last_import_id: int,
        max_chunk_rows: int
    ) -> Optional[DataFrame]:
        """
        Read files from S3 and filter to get the next chunk of data.
        Only reads 'id' and 'raw' columns for optimal performance.
        Converts escaped JSON in 'raw' column to proper JSON format.
        
        Returns:
            Processed DataFrame or None if no data
        """
        if not files_to_read:
            return None
        
        paths = [f"s3://{bucket_name}/{f['key']}" for f in files_to_read]
        
        # Use minimal schema - only id and raw columns
        schema = SchemaDefinitions.get_reservation_schema()
        
        # Read only the required columns
        # df = self.spark.read \
        #     .option("header", "true") \
        #     .schema(schema) \
        #     .csv(paths)

        df = self.spark.read \
            .option("header", "true") \
            .option("escape", '"') \
            .option("multiLine", "true") \
            .option("quote", '"') \
            .schema(schema) \
            .csv(paths)
        
        # FIXED: Properly clean the raw JSON field
        # Step 1: Remove leading and trailing quotes
        # Step 2: Replace escaped double quotes ("") with single quotes (")


        processed_df = (
            df.filter(F.col("id") > last_import_id)
            .orderBy("id")
            .limit(max_chunk_rows)
            .select(
                "id",

                # Extract event timestamp (strip any timezone)
                F.to_timestamp(
                    F.regexp_replace(
                        F.get_json_object(F.col("raw"), "$.messageHeader.timestamp"),
                        "(Z|[+-]\\d{2}:?\\d{2})$",
                        ""
                    ),
                    "yyyy-MM-dd HH:mm:ssSSS"
                ).alias("event_timestamp"),

                # Extract event name
                F.get_json_object(
                    F.col("raw"),
                    "$.eventInfo.name"
                ).alias("event_name"),

                # Clean raw JSON
                F.regexp_replace(
                    F.regexp_replace(F.col("raw"), '^"|"$', ''),
                    '""',
                    '"'
                ).alias("raw")
            )
        )
        
        return processed_df
        
    @staticmethod
    def get_id_range(df: DataFrame) -> Tuple[int, int]:
        """Extract the minimum and maximum ID from a DataFrame."""
        stats = df.select(F.min("id"), F.max("id")).collect()[0]
        return int(stats[0]), int(stats[1])


class StateBuilder:
    """Builds new state objects based on processing results."""
    
    @staticmethod
    def build_new_state(new_last_id: int, files_to_read: List[Dict]) -> Optional[Dict]:
        """
        Build a new state dictionary based on the last processed ID.
        
        Returns:
            New state dict or None if file not found
        """
        # Identify the correct file for this last ID
        final_file = None
        for f in files_to_read:
            if f['start'] <= new_last_id <= f['end']:
                final_file = f
                break
        
        if final_file is None:
            return None
        
        return {
            "last_year": final_file['year'],
            "last_month": final_file['month'],
            "last_day": final_file['day'],
            "last_file_name": final_file['key'],
            "last_import_id": new_last_id
        }


class IncrementalJobRunner:
    """Main orchestrator for the incremental job."""
    
    def __init__(self, config: IncrementalJobConfig, spark_session: SparkSession):
        self.config = config
        self.spark = spark_session
        self.state_manager = S3StateManager(config.BUCKET_NAME, config.STATE_FILE_KEY)
        self.data_processor = DataProcessor(spark_session)
    
    def run(self) -> Tuple[Optional[DataFrame], Optional[int], Optional[int], Optional[str], Optional[str], Optional[str]]:
        """
        Execute the incremental job.
        
        Returns:

        Processed DataFrame with 'id', 'datetime_received', cleaned 'raw' columns, event type, min_id, max_id, year, month, day
        where year/month/day are the values updated in the state JSON.

        or
        
        None, None, None
        """
        state = self.state_manager.get_state()
        current_date = datetime.now()
        
        files_to_read = FileSelector.get_target_files_numerically(
            state,
            current_date,
            self.config.BUCKET_NAME,
            self.config.BASE_PREFIX,
            self.config.MAX_CHUNK_ROWS
        )
        
        if not files_to_read:
            return None, None, None, None, None, None
        
        # Read and process data        
        processed_df = self.data_processor.read_and_filter(
            self.config.BUCKET_NAME,
            files_to_read,
            state['last_import_id'],
            self.config.MAX_CHUNK_ROWS
        )
        
        if processed_df is None:
            return None, None, None, None, None, None
        
        count = processed_df.count()
        
        if count > 0:
            # 1. Calculate the range of IDs in this specific batch
            min_id, max_id = DataProcessor.get_id_range(processed_df)
            
            # 2. Build the new state object based on the max_id found
            # This looks up which S3 partition (year/month/day) the max_id belongs to
            new_state = StateBuilder.build_new_state(max_id, files_to_read)
            
            if new_state:
                # 3. Update S3 state.json
                self.state_manager.update_state(new_state)
                print(f"Processed batch IDs: {min_id} to {max_id} ({count} rows)")
                print(f"Updated to partition: year={new_state['last_year']}, month={new_state['last_month']}, day={new_state['last_day']}")
                
                # 4. Extract the logical partition values from the state we just saved
                year = new_state['last_year']
                month = new_state['last_month']
                day = new_state['last_day']
                
                print(f"Processed batch IDs: {min_id} to {max_id}")
                print(f"State Updated to Partition: year={year}, month={month}, day={day}")
                
                return processed_df, min_id, max_id, year, month, day
        
        return None, None, None, None, None, None


def run_incremental_job(spark: SparkSession) -> Tuple[Optional[DataFrame], Optional[int], Optional[int], Optional[str], Optional[str], Optional[str]]:
    """
    Main entry point for the incremental job.
    Returns a DataFrame with 'id', 'datetime_received', and cleaned 'raw' columns.
    The 'raw' column will have proper JSON format (not escaped), event type, min_id, max_id, year, month, day
    where year/month/day are the values updated in the state JSON.

    or
    
    None, None, None
    """
    config = IncrementalJobConfig()
    
    runner = IncrementalJobRunner(config, spark)
    return runner.run()


# Execute
# if __name__ == "__main__":
#     output_df = run_incremental_job()
#     if output_df:
#         # DataFrame will have 'id', 'datetime_received', and 'raw' columns
#         # The 'raw' column will contain properly formatted JSON
#         row = output_df.first()
#         print(row["raw"])
#         output_df.show(truncate=False)
#         output_df.printSchema()