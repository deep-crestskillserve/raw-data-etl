"""
Business Keys Definition for Database Tables

This module provides business keys for uniquely identifying records in each table.
Business keys are natural identifiers from the business domain that uniquely
identify records without relying on auto-generated UUIDs.

Returns business keys in O(1) time complexity using dictionary lookup.
"""

from typing import List, Optional


# Dictionary mapping table names to their business key columns
# Business keys are combinations of fields that uniquely identify records
BUSINESS_KEYS: dict[str, List[str]] = {
    # Core entity tables
    "hotel": ["hotelId", "hotelCode"],
    "reservedRoom": ["startDate", "endDate", "roomNumber"],
    "travelAgent": ["agentIataId", "name"],
    "company": ["companyId", "corporateId", "name"],
    "group": ["groupId", "hotel_id", "name", "eventInfo_name", "groupInfo_groupLastUpdateTimestamp", "groupInfo_isGroupBlockAutoDrop", "groupInfo_isRateAwardEligible", "groupInfo_isGroupWithCompany", "groupInfo_isElastic", "groupContact_lastName", "groupContact_firstName", "cancelReason"],
    "reservation": ["reservationId", "hotels_id", "eventInfo_name", "arrivalDate", "departureDate", "reservationLastUpdateTimestamp", "reservedRoom", "associations_company_id", "checkOut_group_id", "associations_travelAgent_id", "associations_groups_id" "costSummary_averageNightlyReservedRateAmountBeforeTax", "costSummary_totalTaxAmount", "costSummary_totalOtherRevenueAmountBeforeTax"],
    "confirmationNumber": ["confirmationNumber", "source"],
    "reservationStatus": ["statusTimestamp", "reservationStatus", "confirmationId"],
    "loyaltyRewardsMembership": ["loyaltyNumber", "loyaltyType", "loyaltyLevel"],
    "customer": ["lastName", "firstName", "customerIsPrimary", "customerIsSharing", "customerHasTaxExemption", "customerIsVIP"],
    "contactPurpose": ["purpose"],
    "contact": ["emailAddress", "telephoneNumber_number", "postalAddress_postalCode", "contactIsPrimary", "postalAddress_addressLine1", "postalAddress_addressLine2", "postalAddress_addressLine3"],
    
    # Reservation-related tables
    "reservationItem": ["startDate", "endDate", "itemType", "quantity", "adultCount", "childCount"],
    "reservedInventory": ["startDate", "endDate", "inventoryCode"],
    "reservedRate": ["startDate", "endDate", "ratePlanCode", "rateAmount", "onePersonRate", "twoPersonRate", "rateIsAwardEligible", "rateIsConfidential", "rateIsPackage", "packageNetRoomRateAmount", "rateIsCommissionable"],
    "specialRequest": ["code", "description"],
    
    # Payment and guarantee tables
    "creditCard": ["cardType", "lastFourDigits", "cardIsVirtual"],
    "guarantee": ["guaranteeType", "depositDueAmount", "depositDueDate"],
    "paymentMethod": ["folioId", "folioName", "paymentType"],
    "linkedReservation": ["confirmationNumber", "source"],
    "folioSummary": ["transactionType", "totalTransactionTypeAmount"],
    
    # Group-related tables
    "accountStatus": ["linkTable", "linkId", "statusTimestamp", "status", "operatorId", "confirmationNumber"],
    "groupBlock": ["date", "inventoryCode", "quantity"],
    "groupRate": ["startDate", "endDate", "inventoryType", "onePersonRate", "twoPersonRate", "extrapersonRate"],
    
    # Rate tables
    "rate": ["hotel_id", "rate_update_type", "timestamp", "recap", "recapType", "isTaxInclusive"],
    "ratePlanCode": ["ratePlanCode", "isTaxInclusive"],
    "amountOffRate": ["startDate", "endDate", "inventoryCode", "baseRatePlan", "losMin", "losMax", "onePersonRate", "twoPersonRate", "onePersonAmountOff", "twoPersonAmountOff", "baseOnePersonRate", "baseTwoPersonRate", "limitToDaysOfWeek_sunday", "limitToDaysOfWeek_monday", "limitToDaysOfWeek_tuesday", "limitToDaysOfWeek_wednesday", "limitToDaysOfWeek_thursday", "limitToDaysOfWeek_friday", "limitToDaysOfWeek_saturday"],
    "percentOffRate": ["startDate", "endDate", "inventoryCode", "baseRatePlan", "losMin", "losMax", "onePersonRate", "twoPersonRate", "onePersonPercentOff", "twoPersonPercentOff", "baseOnePersonRate", "baseTwoPersonRate", "limitToDaysOfWeek_sunday", "limitToDaysOfWeek_monday", "limitToDaysOfWeek_tuesday", "limitToDaysOfWeek_wednesday", "limitToDaysOfWeek_thursday", "limitToDaysOfWeek_friday", "limitToDaysOfWeek_saturday"],
    "amountAboveRate": ["startDate", "endDate", "inventoryCode", "baseRatePlan", "losMin", "losMax", "onePersonRate", "twoPersonRate", "onePersonAmountAbove", "twoPersonAmountAbove", "baseOnePersonRate", "baseTwoPersonRate", "limitToDaysOfWeek_sunday", "limitToDaysOfWeek_monday", "limitToDaysOfWeek_tuesday", "limitToDaysOfWeek_wednesday", "limitToDaysOfWeek_thursday", "limitToDaysOfWeek_friday", "limitToDaysOfWeek_saturday"],
    "percentAboveRate": ["startDate", "endDate", "inventoryCode", "baseRatePlan", "losMin", "losMax", "onePersonRate", "twoPersonRate", "onePersonPercentAbove", "twoPersonPercentAbove", "baseOnePersonRate", "baseTwoPersonRate", "limitToDaysOfWeek_sunday", "limitToDaysOfWeek_monday", "limitToDaysOfWeek_tuesday", "limitToDaysOfWeek_wednesday", "limitToDaysOfWeek_thursday", "limitToDaysOfWeek_friday", "limitToDaysOfWeek_saturday"],
    "amountFixedRate": ["startDate", "endDate", "inventoryCode", "losMin", "losMax", "onePersonRate", "twoPersonRate", "limitToDaysOfWeek_sunday", "limitToDaysOfWeek_monday", "limitToDaysOfWeek_tuesday", "limitToDaysOfWeek_wednesday", "limitToDaysOfWeek_thursday", "limitToDaysOfWeek_friday", "limitToDaysOfWeek_saturday"],
    "extraCharge": ["rate_id", "startDate", "endDate", "extraChargeType", "onePersonRate", "twoPersonRate"],
    
    # Inventory tables
    "inventoryBatch": ["hotel_id", "startDate", "endDate", "inventoryCode", "itemType", "availableCount", "outOfOrderCount", "physicalCount", "soldCount", "groupBlockCount", "groupPickUpCount", "overbookingLimit"],
    "inventoryAvailableCount": ["hotel_id", "startDate", "endDate", "inventoryCode", "itemType", "quantity"],
    "inventoryOutOfOrderCount": ["hotel_id", "startDate", "endDate", "inventoryCode", "itemType", "quantity"],
    "inventoryPhysicalCount": ["hotel_id", "startDate", "endDate", "inventoryCode", "itemType", "quantity"],
    
    # Housekeeping and folio tables
    "housekeepingStatus": ["hotel_id", "roomNumber", "roomStatusUpdateTimestamp", "roomStatusUpdateOperatorId", "currentRoomCondition", "previousRoomCondition", "checkInReady"],
    "folioItem": ["transactionTimestamp", "transactionBusinessDate", "transactionCode", "transactionType", "folioId", "transactionAmount", "taxItem"],
    "cleaningSchedule": ["startDate", "endDate", "cleaningType"],
}


def get_business_keys(table_name: str) -> Optional[List[str]]:
    """
    Get business keys for a given table name.
    
    Business keys are natural identifiers from the business domain that uniquely
    identify records without relying on auto-generated UUIDs. These are used
    for deduplication and record identification.
    
    Args:
        table_name: Name of the table (case-insensitive)
        
    Returns:
        List of column names that form the business key, or None if table not found
        
    Time Complexity: O(1) - Dictionary lookup
        
    Examples:
        >>> get_business_keys("hotel")
        ['hotelId', 'hotelCode']
        
        >>> get_business_keys("reservation")
        ['reservationId', 'hotels_id']
        
        >>> get_business_keys("nonexistent")
        None
    """
    # Normalize table name to lowercase for case-insensitive lookup
    # normalized_name = table_name.lower()
    return BUSINESS_KEYS.get(table_name)