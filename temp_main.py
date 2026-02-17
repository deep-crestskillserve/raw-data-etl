# --- START OF FILE main.py ---
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import sys

# OLD IMPORTS
# from incremental_data_loader import run_incremental_job
# from database_connection import create_db_connection_from_secrets

# NEW IMPORTS
from pending_loader import run_direct_load
from direct_database_connection import create_test_db_connection

from event_processor import process_events_by_type 
from audit_dfs import get_audit_dfs
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
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Use the specific S3 URI from job arguments
    input_csv_path = "s3://noble-landing-useast1-183171473439-prod/combined.csv"
    output_base_dir = "s3://noble-landing-useast1-183171473439-prod/processed_output"

    # Initialize connection to test_db
    db_conn = create_test_db_connection(
        secret_name="prod/noble/mysql/noble_db_v2"
    )
    
    # 1. Run Direct Load (Get only the DataFrame)
    input_df = run_direct_load(spark, input_csv_path)

    if input_df is not None:
        input_df = input_df.na.fill({"raw": "{}"}).filter(
            (F.col("raw").isNotNull()) & 
            (F.col("id").isNotNull())
        )
        audit_dfs = get_audit_dfs(spark)
        
        # 2. Process events
        success_tables, failure_tables = process_events_by_type(spark, input_df, audit_dfs)

        def get_clean_tables(table_dict):
            clean = {}
            for t, df in table_dict.items():
                if df is not None:
                    if df.limit(1).count() > 0:
                        clean[t] = df.filter(F.col("id").isNotNull())
            return clean

        final_success = get_clean_tables(success_tables) if success_tables else {}
        final_failure = get_clean_tables(failure_tables) if failure_tables else {}

        # 3. Process Succeeded records (Direct Write to S3 & test_db)
        if final_success:
            # Sort tables by priority for DB integrity
            sorted_tables = sorted(
                final_success.keys(), 
                key=lambda x: TABLE_INSERT_PRIORITY.index(x) if x in TABLE_INSERT_PRIORITY else 999
            )

            for t in sorted_tables:
                df = final_success.get(t)
                if df is None:
                    print(f"Skipping {t}: No DataFrame found.")
                    continue

                if df.limit(1).count() == 0:
                    print(f"Skipping {t}: DataFrame is empty (0 rows).")
                    continue
                print(f"Writing {t} to Success: {df.count()} rows")
                db_conn.write_direct_to_s3(
                    processed_dfs={t: df},
                    base_s3_path=output_base_dir, # Writes to {s3_uri}/succeeded/{table}
                    succeeded=True
                )
        
        # 4. Process Failed records (Direct Write to S3 only)
        if final_failure:
            db_conn.write_direct_to_s3(
                processed_dfs=final_failure,
                base_s3_path=output_base_dir, # Writes to {s3_uri}/failed/{table}
                succeeded=False
            )

    else:
        print("No input data found at specified URI")
    
    job.commit()

if __name__ == "__main__":
    main()