from database_connection import create_db_connection_from_secrets
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
def get_audit_dfs(spark: SparkSession) -> dict:
    # try:
    #     db_source = create_db_connection_from_secrets(
    #         secret_name="prod/noble/mysql/noble_db_v2",
    #         database="noble_db_v2"
    #     )
        
    #     # Initialize as empty dict
    #     audit_dfs = {}
    #     tables = [
    #         "accountStatus",
    #         "amountAboveRate",
    #         "amountFixedRate",
    #         "amountOffRate",
    #         "cleaningSchedule",
    #         "company",
    #         "confirmationNumber",
    #         "contact",
    #         "contactContactPurposeLink",
    #         "contactPurpose",
    #         "creditCard",
    #         "customer",
    #         "customerContactLink",
    #         "customerLoyaltyRewardsMembershipLink",
    #         "extraCharge",
    #         "folioItem",
    #         "folioSummary",
    #         "group",
    #         "groupAccountStatusLink",
    #         "groupBlock",
    #         "groupConfirmationNumberLink",
    #         "groupContactLink",
    #         "groupFolioItemLink",
    #         "groupFolioSummaryLink",
    #         "groupGroupBlockLink",
    #         "groupGroupRateLink",
    #         "groupGuestCreditCardLink",
    #         "groupLoyaltyRewardsMembershipLink",
    #         "groupPaymentMethodLink",
    #         "groupPlannerCreditCardLink",
    #         "groupRate",
    #         "guarantee",
    #         "guaranteeCreditCardLink",
    #         "hotel",
    #         "housekeepingStatus",
    #         "housekeepingStatusConfirmationNumberLink",
    #         "inventoryAvailableCount",
    #         "inventoryBatch",
    #         "inventoryOutOfOrderCount",
    #         "inventoryPhysicalCount",
    #         "linkedReservation",
    #         "loyaltyRewardsMembership",
    #         "paymentMethod",
    #         "paymentMethodCreditCardLink",
    #         "percentAboveRate",
    #         "percentOffRate",
    #         "rate",
    #         "rateAmountAboveRateLink",
    #         "rateAmountFixedRateLink",
    #         "rateAmountOffRateLink",
    #         "rateExtraChargeLink",
    #         "ratePercentAboveRateLink",
    #         "ratePercentOffRateLink",
    #         "ratePlanCode",
    #         "rateRatePlanCodeLink",
    #         "reservation",
    #         "reservationCleaningScheduleLink",
    #         "reservationConfirmationNumberLink",
    #         "reservationCustomerLink",
    #         "reservationEmailAddressLink",
    #         "reservationFolioSummaryLink",
    #         "reservationGuaranteeLink",
    #         "reservationItem",
    #         "reservationItemLink",
    #         "reservationItemReservedInventoryLink",
    #         "reservationItemReservedRateLink",
    #         "reservationItemReservedRoomLink",
    #         "reservationItemSpecialRequestLink",
    #         "reservationLinkedReservationLink",
    #         "reservationPaymentMethodLink",
    #         "reservationStatus",
    #         "reservationStatusLink",
    #         "reservedInventory",
    #         "reservedRate",
    #         "reservedRoom",
    #         "specialRequest",
    #         "travelAgent"
    #     ]

    #     for table in tables:
    #         try:
    #             # Store with the actual table name key used by processors
    #             audit_dfs[table] = db_source.read_table(spark, table).select("id")
    #         except Exception as e:
    #             print(f"Warning: Could not load audit for {table}: {e}")
    #             audit_dfs[table] = None 
                
    #     return audit_dfs
    # except Exception as e:
    #     print(f"Error initializing audit connection: {e}")
    #     return {} # Always return a dictionary
    """
    REDUCED TO ZERO: We no longer pre-load 75 tables.
    The 'audit_dfs' dictionary will now be empty.
    The actual audit will happen lazily inside the Processor.
    """
    print("Optimization: Skipping pre-load of 75 audit tables to save startup time.")
    return {}