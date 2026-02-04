from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import sys
from incremental_data_loader import run_incremental_job
from event_processor import process_events_by_type 
from audit_dfs import get_audit_dfs
from database_connection import create_db_connection_from_secrets
from event_processor import process_events_by_type, handle_null_values 
import pyspark.sql.functions as F



def main():
    """Main execution function for AWS Glue job."""
    # Initialize Glue context
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    db_conn = create_db_connection_from_secrets(
        secret_name="prod/noble/mysql/noble_db_v2",
        database="noble_db_v2"
    )
    
    input_df, start_id, end_id, year, month, day = run_incremental_job(spark)

    if input_df is not None:
        input_df = input_df.na.fill({"raw": "{}"}).filter(
            (F.col("raw").isNotNull()) & 
            (F.col("id").isNotNull())
        )
        audit_dfs = get_audit_dfs(spark)
        # for table_name, audit_df in audit_dfs.items():
        #     if audit_df is not None:
        #         print(f"Audit DataFrame for {table_name}:")
        #         audit_df.show(truncate=False)
        #         print("-" * 100)
        #     else:
        #         print(f"No audit data available for {table_name}")
        # Process sets
        success_tables, failure_tables = process_events_by_type(spark, input_df, audit_dfs)

        def get_clean_tables(table_dict):
            clean = {}
            for t, df in table_dict.items():
                if df is not None:
                    # Use limit(1).count() to check existence without triggering a full NPE-prone scan
                    if df.limit(1).count() > 0:
                        # Drop rows that somehow resulted in a null ID from the UDF
                        clean[t] = df.filter(F.col("id").isNotNull())
            return clean

        final_success = get_clean_tables(success_tables)
        final_failure = get_clean_tables(failure_tables)

        # 1. Process Succeeded records (S3 + RDS)
        if final_success:
            for t, df in final_success.items():
                print(f"Writing {t} to Success: {df.count()} rows")

            db_conn.write_dataframe_to_s3(
                processed_dfs=final_success,
                start_id=start_id, end_id=end_id,
                year=year, month=month, day=day,
                succeeded=True
            )
        
        # 2. Process Failed records (S3 Only)
        if final_failure:
            for t, df in final_failure.items():
                print(f"Writing {t} to Failed: {df.count()} rows")
            db_conn.write_dataframe_to_s3(
                processed_dfs=final_failure,
                start_id=start_id, end_id=end_id,
                year=year, month=month, day=day,
                succeeded=False
            )

    else:
        print("No input data found")
        job.commit()
        sys.exit(0)


if __name__ == "__main__":
    main()