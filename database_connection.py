"""
Database Connection Module

This module provides database connection functionality using AWS Secrets Manager
(for AWS Glue ETL jobs) with support for multiple database connections.
"""

import json
import boto3
from botocore.exceptions import ClientError
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
# ------------------------------------------------------------------
# Initialize Spark & Glue Context
# ------------------------------------------------------------------
# 1. Initialize the Spark Context


def get_db_secret(
    secret_name: str,
    region_name: str = "us-east-1"
) -> Dict[str, Any]:
    """
    Fetch database credentials from AWS Secrets Manager.
    
    Args:
        secret_name: Name or ARN of the secret
        region_name: AWS region
        
    Returns:
        Dictionary containing DB connection details (without database name)
    """
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise RuntimeError(
            f"Unable to fetch secret: {secret_name}"
        ) from e
    
    return json.loads(response["SecretString"])


# ------------------------------------------------------------------
# Database Connection Class
# ------------------------------------------------------------------
class DatabaseConnection:
    """
    Handles database connections and operations using Spark JDBC.
    """
    
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        driver: str = "com.mysql.cj.jdbc.Driver",
        jdbc_url_template: Optional[str] = None
    ):
        """
        Initialize database connection parameters.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            username: Database username
            password: Database password
            driver: JDBC driver class name
            jdbc_url_template: Optional custom JDBC URL template
        """
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        
        if jdbc_url_template:
            self.jdbc_url = jdbc_url_template.format(
                host=host,
                port=port,
                database=database
            )
        else:
            self.jdbc_url = f"jdbc:mysql://{host}:{port}/{database}"
    
    def get_jdbc_properties(self) -> Dict[str, str]:
        """
        Get JDBC connection properties.
        
        Returns:
            Dictionary with JDBC connection properties
        """
        return {
            "driver": self.driver,
            "url": self.jdbc_url,
            "user": self.username,
            "password": self.password
        }
    
    def execute_query(
        self,
        spark: SparkSession,
        query: str
    ) -> DataFrame:
        """
        Execute a SELECT SQL query and return results as a DataFrame.
        Note: Only use SELECT queries. For SHOW commands, use get_tables() instead.
        
        Args:
            spark: SparkSession instance
            query: SQL SELECT query to execute
            
        Returns:
            DataFrame containing query results
        """
        properties = self.get_jdbc_properties()
        
        return spark.read \
            .format("jdbc") \
            .option("url", properties["url"]) \
            .option("query", query) \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .option("driver", properties["driver"]) \
            .load()
    
    def read_table(
        self,
        spark: SparkSession,
        table_name: str,
        schema: Optional[StructType] = None
    ) -> DataFrame:
        """
        Read table from database.
        """
        properties = self.get_jdbc_properties()
        
        # FIX: Wrap table name in backticks for MySQL keywords
        escaped_table_name = f"`{table_name}`"
        reader = spark.read \
            .format("jdbc") \
            .option("url", properties["url"]) \
            .option("dbtable", escaped_table_name) \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .option("driver", properties["driver"])
        
        if schema:
            reader = reader.schema(schema)
        
        return reader.load()
    
    def insert_from_s3_parquet(
        self,
        spark: SparkSession,
        s3_parquet_path: str,
        table_name: str,
        mode: str = "append",
        batch_size: Optional[int] = None,
        auto_batch_size: bool = True
    ) -> None:
        """
        Reads a Parquet file from S3 and inserts its records into a database table.
        
        Args:
            spark: SparkSession instance
            s3_parquet_path: S3 URI (e.g., 's3://bucket/path/batch-123.parquet')
            table_name: Target database table
            mode: Spark write mode ("append" is default for inserts)
            batch_size: Manual JDBC batch size
            auto_batch_size: If True, calculates optimal batch size based on row count
        """
        properties = self.get_jdbc_properties()
        
        # 1. Read the Parquet data from S3
        print(f"Reading Parquet data from: {s3_parquet_path}")
        df = spark.read.parquet(s3_parquet_path)
        
        row_count = df.count()
        if row_count == 0:
            print(f"No records found in {s3_parquet_path}. Skipping insert.")
            return

        # 2. Determine optimal batch size
        if batch_size is None and auto_batch_size:
            if row_count < 10000:
                batch_size = 1000
            elif row_count < 100000:
                batch_size = 5000
            else:
                batch_size = 10000
        elif batch_size is None:
            batch_size = 1000  # Fallback default
            
        print(f"Inserting {row_count} records into table `{table_name}` (Batch Size: {batch_size})")

        if(row_count == 0):
            print(f"No records found in {s3_parquet_path}. Skipping insert.")
            return

        # 3. Write to Database
        # MySQL specific: Use backticks for the table name in case it's a keyword like 'group'
        escaped_table_name = f"`{table_name}`"

        try:
            df.write \
                .format("jdbc") \
                .option("url", properties["url"]) \
                .option("dbtable", escaped_table_name) \
                .option("user", properties["user"]) \
                .option("password", properties["password"]) \
                .option("driver", properties["driver"]) \
                .option("batchsize", batch_size) \
                .option("rewriteBatchedStatements", "true") \
                .mode(mode) \
                .save()
            print(f"Successfully inserted {row_count} data into {table_name}")
            
        except Exception as e:
            print(f"Error during bulk insert to {table_name}: {str(e)}")
            raise e

    def write_dataframe_to_s3(
        self,
        processed_dfs: dict[str, DataFrame],
        start_id: int,
        end_id: int,
        year: str,
        month: str,
        day: str,
        succeeded: bool = True,
    ) -> None:
        """
        Writes a dictionary of DataFrames to S3 in Parquet format using a structured partitioning scheme.
        
        Args:
            processed_dfs: Dictionary mapping table names to Spark DataFrames
            start_id: The first ID in the processed batch
            end_id: The last ID in the processed batch
            year: Logical partition year
            month: Logical partition month
            day: Logical partition day
            succeeded: Status flag to determine the S3 folder path (succeeded/failed)
        """
        base_s3_path = "s3://noble-stage-useast1-183171473439-prod/source=sky-touch-raw/schema=reservation"

        for table_name, table_df in processed_dfs.items():
            # Construct the specific S3 URI for this table and ID range
            # Note: We use table_name (e.g., 'hotel') inside the path
            mode = "overwrite"
            status = "succeeded" if succeeded else "failed"
            
            s3_uri = f"{base_s3_path}/year={year}/month={month}/day={day}/{status}/{table_name}/{start_id}-{end_id}.parquet"

            print(f"Writing {table_name} to S3: {s3_uri}")

            try:
                # Check if DataFrame has data before writing
                has_data = table_df.limit(1).count() > 0
                if has_data:
                    print(f"Writing {table_name} ({status}) to S3: {s3_uri}")
                    table_df.write.mode(mode).parquet(s3_uri)
                    
                    # ONLY insert into DB if records are valid/succeeded
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
                print(f"Error writing {table_name} to S3: {str(e)}")
                raise e

# ------------------------------------------------------------------
# Factory Function (Secrets Manager Based)
# ------------------------------------------------------------------
def create_db_connection_from_secrets(
    secret_name: str,
    database: str,
    region_name: str = "us-east-1",
    driver: str = "com.mysql.cj.jdbc.Driver"
) -> DatabaseConnection:
    """
    Factory function to create DatabaseConnection using AWS Secrets Manager.
    Database name is provided as a parameter instead of being retrieved from secrets.
    
    Args:
        secret_name: Secrets Manager name or ARN (contains host, port, username, password)
        database: Database name to connect to
        region_name: AWS region
        driver: JDBC driver class name
        
    Returns:
        DatabaseConnection instance
    """
    secret = get_db_secret(secret_name, region_name)
    
    return DatabaseConnection(
        host=secret["host"],
        port=int(secret["port"]),
        database=database,
        username=secret["username"],
        password=secret["password"],
        driver=secret.get("driver", driver)
    )