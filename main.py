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

TABLE_INSERT_PRIORITY = [
    # LAYER 0: Independent Base Entities
    "hotel",
    "confirmationNumber",
    "reservationStatus",
    "customer",
    "contact",
    "contactPurpose",
    "travelAgent",
    "company",
    "reservedRoom",
    "reservationItem",
    "reservedInventory",
    "reservedRate",
    "specialRequest",
    "creditCard",
    "guarantee",
    "paymentMethod",
    "loyaltyRewardsMembership",
    "linkedReservation",
    "folioSummary",
    "folioItem",
    "accountStatus",
    "groupBlock",
    "groupRate",
    "cleaningSchedule",
    "ratePlanCode",
    "amountOffRate",
    "percentOffRate",
    "amountAboveRate",
    "percentAboveRate",
    "amountFixedRate",
    "extraCharge",
    "bookingChannel",

    # LAYER 1: Entities dependent on Layer 0
    "group",              # FKs: hotel, company, travelAgent
    "housekeepingStatus", # FKs: hotel
    "inventoryBatch",     # FKs: hotel
    "inventoryAvailableCount", # FKs: hotel
    "inventoryOutOfOrderCount", # FKs: hotel
    "inventoryPhysicalCount",   # FKs: hotel
    "rate",               # FKs: hotel

    # LAYER 2: Entities dependent on Layer 0 and Layer 1
    "reservation",        # FKs: hotel, reservedRoom, travelAgent, group, company

    # LAYER 3: Link Tables (Must be last)
    "reservationConfirmationNumberLink",
    "reservationStatusLink",
    "reservationCustomerLink",
    "customerLoyaltyRewardsMembershipLink",
    "customerContactLink",
    "contactContactPurposeLink",
    "groupContactLink",
    "reservationEmailAddressLink",
    "reservationItemReservedInventoryLink",
    "reservationItemReservedRateLink",
    "reservationItemReservedRoomLink",
    "reservationItemSpecialRequestLink",
    "reservationItemLink",
    "guaranteeCreditCardLink",
    "reservationGuaranteeLink",
    "paymentMethodCreditCardLink",
    "reservationPaymentMethodLink",
    "reservationLinkedReservationLink",
    "reservationFolioSummaryLink",
    "groupFolioSummaryLink",
    "groupAccountStatusLink",
    "groupConfirmationNumberLink",
    "groupGroupBlockLink",
    "groupGroupRateLink",
    "groupPlannerCreditCardLink",
    "groupGuestCreditCardLink",
    "groupPaymentMethodLink",
    "groupLoyaltyRewardsMembershipLink",
    "rateRatePlanCodeLink",
    "rateAmountOffRateLink",
    "ratePercentOffRateLink",
    "rateAmountAboveRateLink",
    "ratePercentAboveRateLink",
    "rateAmountFixedRateLink",
    "rateExtraChargeLink",
    "housekeepingStatusConfirmationNumberLink",
    "groupFolioItemLink",
    "reservationCleaningScheduleLink"
]

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

        if(success_tables is not None):
            final_success = get_clean_tables(success_tables)
        else:
            final_success = None

        if(failure_tables is not None):
            final_failure = get_clean_tables(failure_tables)
        else:
            final_failure = None

        # 1. Process Succeeded records (S3 + RDS)
        if final_success:
            # Sort tables based on priority list, others at the end
            sorted_tables = sorted(
                final_success.keys(), 
                key=lambda x: TABLE_INSERT_PRIORITY.index(x) if x in TABLE_INSERT_PRIORITY else 999
            )

            for t in sorted_tables:
                df = final_success.get(t)

                # Check if df is None
                if df is None:
                    print(f"Skipping {t}: No DataFrame found.")
                    continue
                    
                # Check if df is empty before calling write_dataframe_to_s3
                # This saves time and avoids logging "Success" for 0 rows
                if df.limit(1).count() == 0:
                    print(f"Skipping {t}: DataFrame is empty (0 rows).")
                    continue
                print(f"Writing {t} to Success: {df.count()} rows")
                db_conn.write_dataframe_to_s3(
                    processed_dfs={t: df},
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