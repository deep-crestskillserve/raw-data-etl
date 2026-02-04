from database_connection import create_db_connection_from_secrets
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
def get_audit_dfs(spark: SparkSession) -> dict:
    try:
        db_source = create_db_connection_from_secrets(
            secret_name="prod/noble/mysql/noble_db_v2",
            database="noble_db_v2"
        )
        
        # Initialize as empty dict
        audit_dfs = {}
        tables = [
            "accountStatus", "amountAboveRate", "amountFixedRate", "amountOffRate",
            "cleaningSchedule", "company", "confirmationNumber", "contactPurpose",
            "contact", "creditCard", "customer", "extraCharge", "folioItem",
            "folioSummary", "groupBlock", "groupRate", "group", "guarantee",
            "hotel", "housekeepingStatus", "inventoryAvailableCount", "inventoryBatch",
            "inventoryOutOfOrderCount", "inventoryPhysicalCount", "linkedReservation",
            "loyaltyRewardsMembership", "paymentMethod", "percentAboveRate",
            "percentOffRate", "ratePlanCode", "rate", "reservationItem",
            "reservationStatus", "reservation", "reservedInventory", "reservedRate",
            "reservedRoom", "specialRequest", "travelAgent"
        ]

        for table in tables:
            try:
                # Store with the actual table name key used by processors
                audit_dfs[table] = db_source.read_table(spark, table).select("id")
            except Exception as e:
                print(f"Warning: Could not load audit for {table}: {e}")
                audit_dfs[table] = None 
                
        return audit_dfs
    except Exception as e:
        print(f"Error initializing audit connection: {e}")
        return {} # Always return a dictionary