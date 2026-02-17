# --- START OF FILE pending_loader.py ---
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from incremental_data_loader import SchemaDefinitions

def run_direct_load(spark: SparkSession, s3_uri: str) -> DataFrame:
    """
    Reads all CSV data from a specific S3 URI, cleans the raw JSON, 
    and returns a single DataFrame. No state or ID tracking.
    """
    print(f"Direct Load: Reading data from {s3_uri}")
    
    schema = SchemaDefinitions.get_reservation_schema()
    
    # Read all CSV files at the URI
    df = spark.read \
        .option("header", "true") \
        .option("escape", '"') \
        .option("multiLine", "true") \
        .option("quote", '"') \
        .schema(schema) \
        .csv(s3_uri)

    print("df dataframe")
    # df_raw = df.select("raw")
    # df_raw.show(20, truncate=False)
    # Apply the same cleaning logic as the incremental loader
    processed_df = df.select(
        "id",
        # Extract event timestamp
        F.to_timestamp(
            F.regexp_replace(
                F.get_json_object(F.col("raw"), "$.messageHeader.timestamp"),
                "(Z|[+-]\\d{2}:?\\d{2})$",
                ""
            ),
            "yyyy-MM-dd HH:mm:ssSSS"
        ).alias("event_timestamp"),

        # Extract event name
        F.get_json_object(F.col("raw"), "$.eventInfo.name").alias("event_name"),

        # Raw JSON as-is — CSV reader's escape='"' and quote='"' options
        # should already handle unescaping correctly. Verify with:
        # df.select("raw").show(5, truncate=False)
        F.col("raw")

    ).filter(F.col("id").isNotNull())

    return processed_df