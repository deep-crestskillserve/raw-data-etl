# --- START OF FILE direct_database_connection.py ---
import boto3
import json
from typing import Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from database_connection import DatabaseConnection, get_db_secret

class DirectDatabaseConnection(DatabaseConnection):
    """
    Simplified writer that skips time-based partitioning and 
    targets a specific database.
    """
    
    def write_direct_to_s3(
        self,
        processed_dfs: Dict[str, DataFrame],
        base_s3_path: str,
        succeeded: bool = True,
    ) -> None:
        """
        Writes DataFrames to S3 organized by status and table name only.
        Path: {base_s3_path}/{succeeded|failed}/{table_name}/data.parquet
        """
        status = "succeeded" if succeeded else "failed"
        
        # Ensure base path doesn't end with slash for consistent joining
        base_path = base_s3_path.rstrip('/')

        for table_name, table_df in processed_dfs.items():
            try:
                if table_df.limit(1).count() > 0:
                    s3_uri = f"{base_path}/{status}/{table_name}/data.parquet"
                    print(f"Writing {table_name} ({status}) to S3: {s3_uri}")
                    
                    # Write to S3
                    table_df.write.mode("overwrite").parquet(s3_uri)
                    
                    # Only insert into DB if records succeeded
                    if succeeded:
                        self.insert_from_s3_parquet(
                            spark=table_df.sparkSession,
                            s3_parquet_path=s3_uri,
                            table_name=table_name,
                            mode="append"
                        )
                else:
                    print(f"Skipping {table_name}: DataFrame is empty.")
            except Exception as e:
                print(f"Error processing {table_name} for S3/DB: {str(e)}")
                raise e

def create_test_db_connection(
    secret_name: str,
    region_name: str = "us-east-1"
) -> DirectDatabaseConnection:
    """
    Factory function specifically for the 'test_db' requirement.
    """
    secret = get_db_secret(secret_name, region_name)
    
    return DirectDatabaseConnection(
        host=secret["host"],
        port=int(secret["port"]),
        # database="test_db",  # Hardcoded as requested
        database="noble_db_v2",
        username=secret["username"],
        password=secret["password"]
    )