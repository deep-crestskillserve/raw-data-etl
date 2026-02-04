"""
PySpark schemas for all database tables from prod-db-tables/v7.sql

This module provides PySpark StructType schemas for all tables and a function
to retrieve schemas in O(1) time complexity.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType,
    TimestampType,
    BooleanType,
    BinaryType,
    LongType,
)

# Define all table schemas
SCHEMAS = {
    "hotel": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("hotelId", StringType(), nullable=False),
        StructField("hotelCode", StringType(), nullable=False),
        StructField("propertyCode", StringType(), nullable=False),
        StructField("timeOffsetFromUTC", StringType(), nullable=False),
        StructField("propertyName", StringType(), nullable=False),
        StructField("brandCode", StringType(), nullable=False),
        StructField("currencyCode", StringType(), nullable=False),
        StructField("propertyName_abbrev", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservedRoom": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("startDate", DateType(), nullable=False),
        StructField("endDate", DateType(), nullable=False),
        StructField("roomNumber", StringType(), nullable=False),
        StructField("import_source", StringType(), nullable=True),
        StructField("import_batch_id", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "travelAgent": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("agentIataId", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "company": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("companyId", StringType(), nullable=True),
        StructField("corporateId", StringType(), nullable=True),
        StructField("import_source", StringType(), nullable=True),
        StructField("import_batch_id", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "group": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("eventInfo_name", StringType(), nullable=True),
        StructField("eventInfo_version", StringType(), nullable=True),
        StructField("groupId", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("hotel_id", StringType(), nullable=True),
        StructField("groupStayInfo_arrivalDate", DateType(), nullable=True),
        StructField("groupStayInfo_departureDate", DateType(), nullable=True),
        StructField("groupStayInfo_expectedArrivalTime", StringType(), nullable=True),
        StructField("groupStayInfo_expectedDepartureTime", StringType(), nullable=True),
        StructField("hotelPolicy_guaranteePolicy", StringType(), nullable=True),
        StructField("hotelPolicy_cancellationPolicy", StringType(), nullable=True),
        StructField("company_id", StringType(), nullable=True),
        StructField("groupInfo_isGroupFolioActive", BooleanType(), nullable=True),
        StructField("groupInfo_groupLastUpdateTimestamp", TimestampType(), nullable=True),
        StructField("groupInfo_groupLastUpdateOperatorId", StringType(), nullable=True),
        StructField("groupInfo_groupChargeRouting", StringType(), nullable=True),
        StructField("groupInfo_isCommissionable", BooleanType(), nullable=True),
        StructField("groupInfo_isGroupRateSupressed", BooleanType(), nullable=True),
        StructField("groupInfo_languagePreference", StringType(), nullable=True),
        StructField("groupInfo_salesManagerName", StringType(), nullable=True),
        StructField("groupInfo_groupMarketCode", StringType(), nullable=True),
        StructField("groupInfo_groupTrackingCode", StringType(), nullable=True),
        StructField("groupInfo_isGroupTaxExempt", BooleanType(), nullable=True),
        StructField("groupInfo_isGroupWithRecurringCharges", BooleanType(), nullable=True),
        StructField("groupInfo_groupContractDueDate", DateType(), nullable=True),
        StructField("groupInfo_groupRoomingListDueDate", DateType(), nullable=True),
        StructField("groupInfo_groupContractReceivedDate", DateType(), nullable=True),
        StructField("groupInfo_groupFixedCutoffDate", DateType(), nullable=True),
        StructField("groupInfo_groupRollingCutoffNumberOfDays", StringType(), nullable=True),
        StructField("groupInfo_isGroupBlockAutoDrop", BooleanType(), nullable=True),
        StructField("groupInfo_isRateAwardEligible", BooleanType(), nullable=True),
        StructField("groupInfo_isGroupWithCompany", BooleanType(), nullable=True),
        StructField("groupInfo_isElastic", BooleanType(), nullable=True),
        StructField("groupInfo_groupNonRoomPackageName", StringType(), nullable=True),
        StructField("groupContact_lastName", StringType(), nullable=True),
        StructField("groupContact_firstName", StringType(), nullable=True),
        StructField("groupGuarantee_groupPlannerGuarantee_guaranteeType", StringType(), nullable=True),
        StructField("groupGuarantee_groupPlannerGuarantee_depositDueAmount", DecimalType(12, 2), nullable=True),
        StructField("groupGuarantee_groupPlannerGuarantee_depositDueDate", DateType(), nullable=True),
        StructField("groupGuarantee_guestGuarantee_guaranteeType", StringType(), nullable=True),
        StructField("groupGuarantee_guestGuarantee_depositDueAmount", DecimalType(12, 2), nullable=True),
        StructField("groupGuarantee_guestGuarantee_depositDueDate", DateType(), nullable=True),
        StructField("note_noteType", StringType(), nullable=True),
        StructField("note_noteText", StringType(), nullable=True),
        StructField("travelagent_id", StringType(), nullable=True),
        StructField("cancelReason", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservation": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("eventInfo_name", StringType(), nullable=True),
        StructField("eventInfo_version", StringType(), nullable=True),
        StructField("hotels_id", StringType(), nullable=False),
        StructField("reservationId", StringType(), nullable=True),
        StructField("arrivalDate", DateType(), nullable=True),
        StructField("departureDate", DateType(), nullable=True),
        StructField("hotelPolicy_guaranteePolicy", StringType(), nullable=True),
        StructField("hotelPolicy_cancellationPolicy", StringType(), nullable=True),
        StructField("marketSegment", StringType(), nullable=True),
        StructField("reservedRoom", StringType(), nullable=True),
        StructField("associations_travelAgent_id", StringType(), nullable=True),
        StructField("associations_groups_id", StringType(), nullable=True),
        StructField("associations_company_id", StringType(), nullable=True),
        StructField("sourceOfBusiness_local", BooleanType(), nullable=True),
        StructField("sourceOfBusiness_callerName", StringType(), nullable=True),
        StructField("sourceOfBusiness_externalSource_sourceId", IntegerType(), nullable=True),
        StructField("sourceOfBusiness_externalSource_sourceDescription", StringType(), nullable=True),
        StructField("sourceOfBusiness_localSource_walkIn", BooleanType(), nullable=True),
        StructField("costSummary_averageNightlyReservedRateAmountBeforeTax", DecimalType(12, 2), nullable=True),
        StructField("costSummary_totalTaxAmount", DecimalType(12, 2), nullable=True),
        StructField("costSummary_totalOtherRevenueAmountBeforeTax", DecimalType(12, 2), nullable=True),
        StructField("roomCondition", StringType(), nullable=True),
        StructField("reservationLastUpdateTimestamp", TimestampType(), nullable=True),
        StructField("reservationLastUpdateOperatorId", StringType(), nullable=True),
        StructField("cancellationStatus_statusTimestamp", TimestampType(), nullable=True),
        StructField("cancellationStatus_reservationStatus", StringType(), nullable=True),
        StructField("cancellationStatus_operatorId", StringType(), nullable=True),
        StructField("cancellationStatus_confirmationId", StringType(), nullable=True),
        StructField("cancellationStatus_callerName", StringType(), nullable=True),
        StructField("checkedOutRoom_id", StringType(), nullable=True),
        StructField("checkOut_group_id", StringType(), nullable=True),
        StructField("eCheckIn_guestLastName", StringType(), nullable=True),
        StructField("eCheckIn_eCheckInProcessed", BooleanType(), nullable=True),
        StructField("import_source", StringType(), nullable=True),
        StructField("import_batch_id", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "confirmationNumber": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("confirmationNumber", StringType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("import_source", StringType(), nullable=True),
        StructField("import_batch_id", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservationConfirmationNumberLink": StructType([
        StructField("reservation_id", StringType(), nullable=False),
        StructField("confirmation_number_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "groupConfirmationNumberLink": StructType([
        StructField("group_id", StringType(), nullable=False),
        StructField("confirmation_number_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservationStatus": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("statusTimestamp", TimestampType(), nullable=False),
        StructField("reservationStatus", StringType(), nullable=False),
        StructField("operatorId", StringType(), nullable=True),
        StructField("confirmationId", IntegerType(), nullable=True),
        StructField("callerName", StringType(), nullable=True),
        StructField("import_source", StringType(), nullable=True),
        StructField("import_batch_id", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservationStatusLink": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("status_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "loyaltyRewardsMembership": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("loyaltyType", StringType(), nullable=True),
        StructField("loyaltyNumber", StringType(), nullable=True),
        StructField("loyaltyLevel", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "customer": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("title", StringType(), nullable=True),
        StructField("lastName", StringType(), nullable=False),
        StructField("firstName", StringType(), nullable=False),
        StructField("middleName", StringType(), nullable=True),
        StructField("languagePreference", StringType(), nullable=True),
        StructField("customerIsPrimary", BooleanType(), nullable=True),
        StructField("customerIsSharing", BooleanType(), nullable=True),
        StructField("customerHasTaxExemption", BooleanType(), nullable=True),
        StructField("customerIsVIP", BooleanType(), nullable=True),
        StructField("import_source", StringType(), nullable=True),
        StructField("import_batch_id", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "customerLoyaltyRewardsMembershipLink": StructType([
        StructField("customer_id", StringType(), nullable=False),
        StructField("loyalty_rewards_membership_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "contactPurpose": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("purpose", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "contact": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("optedIn", BooleanType(), nullable=True),
        StructField("contactIsPrimary", BooleanType(), nullable=True),
        StructField("postalAddress_addressLine1", StringType(), nullable=True),
        StructField("postalAddress_addressLine2", StringType(), nullable=True),
        StructField("postalAddress_addressLine3", StringType(), nullable=True),
        StructField("postalAddress_city", StringType(), nullable=True),
        StructField("postalAddress_state", StringType(), nullable=True),
        StructField("postalAddress_postalCode", StringType(), nullable=True),
        StructField("postalAddress_county", StringType(), nullable=True),
        StructField("postalAddress_country", StringType(), nullable=True),
        StructField("telephoneNumber_type", StringType(), nullable=True),
        StructField("telephoneNumber_number", StringType(), nullable=True),
        StructField("emailAddress", StringType(), nullable=True),
        StructField("import_source", StringType(), nullable=True),
        StructField("import_batch_id", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "contactContactPurposeLink": StructType([
        StructField("contact_id", StringType(), nullable=False),
        StructField("contact_purpose_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "customerContactLink": StructType([
        StructField("customer_id", StringType(), nullable=False),
        StructField("contact_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "groupContactLink": StructType([
        StructField("group_id", StringType(), nullable=False),
        StructField("contact_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservationCustomerLink": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservationEmailAddressLink": StructType([
        StructField("reservation_id", StringType(), nullable=False),
        StructField("email_address_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservationItem": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("startDate", DateType(), nullable=False),
        StructField("endDate", DateType(), nullable=False),
        StructField("itemType", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("adultCount", IntegerType(), nullable=True),
        StructField("childCount", IntegerType(), nullable=True),
        StructField("import_source", StringType(), nullable=True),
        StructField("import_batch_id", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservedInventory": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("startDate", DateType(), nullable=False),
        StructField("endDate", DateType(), nullable=False),
        StructField("inventoryCode", StringType(), nullable=False),
        StructField("import_source", StringType(), nullable=True),
        StructField("import_batch_id", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservationItemReservedInventoryLink": StructType([
        StructField("reservation_item_id", StringType(), nullable=False),
        StructField("reserved_inventory_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservedRate": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("startDate", DateType(), nullable=False),
        StructField("endDate", DateType(), nullable=False),
        StructField("rateAmount", DecimalType(12, 2), nullable=True),
        StructField("ratePlanCode", StringType(), nullable=True),
        StructField("ratePlanDescription", StringType(), nullable=True),
        StructField("rateIsAwardEligible", BooleanType(), nullable=True),
        StructField("rateIsConfidential", BooleanType(), nullable=True),
        StructField("rateIsPackage", BooleanType(), nullable=True),
        StructField("packageNetRoomRateAmount", DecimalType(12, 2), nullable=True),
        StructField("rateIsCommissionable", BooleanType(), nullable=True),
        StructField("import_source", StringType(), nullable=True),
        StructField("import_batch_id", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservationItemReservedRateLink": StructType([
        StructField("reservation_item_id", StringType(), nullable=False),
        StructField("reserved_rate_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservationItemReservedRoomLink": StructType([
        StructField("reservation_item_id", StringType(), nullable=False),
        StructField("reserved_room_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "specialRequest": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("code", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("notes", StringType(), nullable=True),
        StructField("import_source", StringType(), nullable=True),
        StructField("import_batch_id", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservationItemSpecialRequestLink": StructType([
        StructField("reservation_item_id", StringType(), nullable=False),
        StructField("special_request_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservationItemLink": StructType([
        StructField("reservation_id", StringType(), nullable=False),
        StructField("reservation_item_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "creditCard": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("cardIsVirtual", BooleanType(), nullable=True),
        StructField("cardType", StringType(), nullable=True),
        StructField("lastFourDigits", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "guarantee": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("guaranteeType", StringType(), nullable=True),
        StructField("depositDueAmount", DecimalType(12, 2), nullable=True),
        StructField("depositDueDate", DateType(), nullable=True),
        StructField("import_source", StringType(), nullable=True),
        StructField("import_batch_id", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "guaranteeCreditCardLink": StructType([
        StructField("guarantee_id", StringType(), nullable=False),
        StructField("credit_card_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservationGuaranteeLink": StructType([
        StructField("reservation_id", StringType(), nullable=False),
        StructField("guarantee_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "paymentMethod": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("folioId", StringType(), nullable=True),
        StructField("folioName", StringType(), nullable=True),
        StructField("paymentType", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "paymentMethodCreditCardLink": StructType([
        StructField("payment_method_id", StringType(), nullable=False),
        StructField("credit_card_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservationPaymentMethodLink": StructType([
        StructField("reservation_id", StringType(), nullable=False),
        StructField("payment_method_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "linkedReservation": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("confirmationNumber", StringType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservationLinkedReservationLink": StructType([
        StructField("reservation_id", StringType(), nullable=False),
        StructField("linked_reservation_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "folioSummary": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("transactionType", StringType(), nullable=False),
        StructField("totalTransactionTypeAmount", DecimalType(12, 2), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservationFolioSummaryLink": StructType([
        StructField("reservation_id", StringType(), nullable=False),
        StructField("folio_summary_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "groupFolioSummaryLink": StructType([
        StructField("group_id", StringType(), nullable=False),
        StructField("folio_summary_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "accountStatus": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("linkTable", StringType(), nullable=True),
        StructField("linkId", StringType(), nullable=True),
        StructField("statusTimestamp", TimestampType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("operatorId", StringType(), nullable=False),
        StructField("confirmationNumber", StringType(), nullable=False),
        StructField("callerName", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "groupAccountStatusLink": StructType([
        StructField("group_id", StringType(), nullable=False),
        StructField("account_status_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "groupBlock": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("date", DateType(), nullable=True),
        StructField("inventoryCode", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("pickupQuantity", IntegerType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "groupGroupBlockLink": StructType([
        StructField("group_id", StringType(), nullable=False),
        StructField("group_block_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "groupRate": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("startDate", DateType(), nullable=True),
        StructField("endDate", DateType(), nullable=True),
        StructField("inventoryType", StringType(), nullable=True),
        StructField("onePersonRate", DecimalType(12, 2), nullable=True),
        StructField("twoPersonRate", DecimalType(12, 2), nullable=True),
        StructField("extrapersonRate", DecimalType(12, 2), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "groupGroupRateLink": StructType([
        StructField("group_id", StringType(), nullable=False),
        StructField("group_rate_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "groupPlannerCreditCardLink": StructType([
        StructField("group_id", StringType(), nullable=False),
        StructField("credit_card_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "groupGuestCreditCardLink": StructType([
        StructField("group_id", StringType(), nullable=False),
        StructField("credit_card_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "groupPaymentMethodLink": StructType([
        StructField("group_id", StringType(), nullable=False),
        StructField("payment_method_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "groupLoyaltyRewardsMembershipLink": StructType([
        StructField("group_id", StringType(), nullable=False),
        StructField("loyalty_rewards_membership_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "rate": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("recap", BooleanType(), nullable=True),
        StructField("hotel_id", StringType(), nullable=False),
        StructField("rate_update_type", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("recapType", StringType(), nullable=True),
        StructField("isTaxInclusive", BooleanType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "ratePlanCode": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("ratePlanCode", StringType(), nullable=True),
        StructField("isTaxInclusive", BooleanType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "rateRatePlanCodeLink": StructType([
        StructField("rate_id", StringType(), nullable=False),
        StructField("rate_plan_code_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "amountOffRate": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("losMin", IntegerType(), nullable=True),
        StructField("losMax", IntegerType(), nullable=True),
        StructField("startDate", DateType(), nullable=True),
        StructField("endDate", DateType(), nullable=True),
        StructField("inventoryCode", StringType(), nullable=True),
        StructField("onePersonRate", DecimalType(12, 2), nullable=True),
        StructField("twoPersonRate", DecimalType(12, 2), nullable=True),
        StructField("onePersonAmountOff", DecimalType(12, 2), nullable=True),
        StructField("twoPersonAmountOff", DecimalType(12, 2), nullable=True),
        StructField("baseRatePlan", StringType(), nullable=True),
        StructField("baseOnePersonRate", DecimalType(12, 2), nullable=True),
        StructField("baseTwoPersonRate", DecimalType(12, 2), nullable=True),
        StructField("limitToDaysOfWeek_sunday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_monday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_tuesday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_wednesday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_thursday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_friday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_saturday", BooleanType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "rateAmountOffRateLink": StructType([
        StructField("rate_id", StringType(), nullable=False),
        StructField("amount_off_rate_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "percentOffRate": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("losMin", IntegerType(), nullable=True),
        StructField("losMax", IntegerType(), nullable=True),
        StructField("startDate", DateType(), nullable=True),
        StructField("endDate", DateType(), nullable=True),
        StructField("inventoryCode", StringType(), nullable=True),
        StructField("onePersonRate", DecimalType(12, 2), nullable=True),
        StructField("twoPersonRate", DecimalType(12, 2), nullable=True),
        StructField("onePersonPercentOff", DecimalType(5, 2), nullable=True),
        StructField("twoPersonPercentOff", DecimalType(5, 2), nullable=True),
        StructField("baseRatePlan", StringType(), nullable=True),
        StructField("baseOnePersonRate", DecimalType(12, 2), nullable=True),
        StructField("baseTwoPersonRate", DecimalType(12, 2), nullable=True),
        StructField("limitToDaysOfWeek_sunday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_monday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_tuesday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_wednesday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_thursday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_friday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_saturday", BooleanType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "ratePercentOffRateLink": StructType([
        StructField("rate_id", StringType(), nullable=False),
        StructField("percent_off_rate_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "amountAboveRate": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("losMin", IntegerType(), nullable=True),
        StructField("losMax", IntegerType(), nullable=True),
        StructField("startDate", DateType(), nullable=True),
        StructField("endDate", DateType(), nullable=True),
        StructField("inventoryCode", StringType(), nullable=True),
        StructField("onePersonRate", DecimalType(12, 2), nullable=True),
        StructField("twoPersonRate", DecimalType(12, 2), nullable=True),
        StructField("onePersonAmountAbove", DecimalType(12, 2), nullable=True),
        StructField("twoPersonAmountAbove", DecimalType(12, 2), nullable=True),
        StructField("baseRatePlan", StringType(), nullable=True),
        StructField("baseOnePersonRate", DecimalType(12, 2), nullable=True),
        StructField("baseTwoPersonRate", DecimalType(12, 2), nullable=True),
        StructField("limitToDaysOfWeek_sunday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_monday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_tuesday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_wednesday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_thursday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_friday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_saturday", BooleanType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "rateAmountAboveRateLink": StructType([
        StructField("rate_id", StringType(), nullable=False),
        StructField("amount_above_rate_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "percentAboveRate": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("losMin", IntegerType(), nullable=True),
        StructField("losMax", IntegerType(), nullable=True),
        StructField("startDate", DateType(), nullable=True),
        StructField("endDate", DateType(), nullable=True),
        StructField("inventoryCode", StringType(), nullable=True),
        StructField("onePersonRate", DecimalType(12, 2), nullable=True),
        StructField("twoPersonRate", DecimalType(12, 2), nullable=True),
        StructField("onePersonPercentAbove", DecimalType(5, 2), nullable=True),
        StructField("twoPersonPercentAbove", DecimalType(5, 2), nullable=True),
        StructField("baseRatePlan", StringType(), nullable=True),
        StructField("baseOnePersonRate", DecimalType(12, 2), nullable=True),
        StructField("baseTwoPersonRate", DecimalType(12, 2), nullable=True),
        StructField("limitToDaysOfWeek_sunday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_monday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_tuesday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_wednesday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_thursday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_friday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_saturday", BooleanType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "ratePercentAboveRateLink": StructType([
        StructField("rate_id", StringType(), nullable=False),
        StructField("percent_above_rate_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "amountFixedRate": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("losMin", IntegerType(), nullable=True),
        StructField("losMax", IntegerType(), nullable=True),
        StructField("startDate", DateType(), nullable=True),
        StructField("endDate", DateType(), nullable=True),
        StructField("inventoryCode", StringType(), nullable=True),
        StructField("onePersonRate", DecimalType(12, 2), nullable=True),
        StructField("twoPersonRate", DecimalType(12, 2), nullable=True),
        StructField("limitToDaysOfWeek_sunday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_monday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_tuesday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_wednesday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_thursday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_friday", BooleanType(), nullable=True),
        StructField("limitToDaysOfWeek_saturday", BooleanType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "rateAmountFixedRateLink": StructType([
        StructField("rate_id", StringType(), nullable=False),
        StructField("amount_fixed_rate_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "extraCharge": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("rate_id", StringType(), nullable=True),
        StructField("startDate", DateType(), nullable=True),
        StructField("endDate", DateType(), nullable=True),
        StructField("extraChargeType", StringType(), nullable=True),
        StructField("rateAmount", DecimalType(12, 2), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "rateExtraChargeLink": StructType([
        StructField("rate_id", StringType(), nullable=False),
        StructField("extra_charge_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "inventoryBatch": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("hotel_id", StringType(), nullable=False),
        StructField("startDate", DateType(), nullable=False),
        StructField("endDate", DateType(), nullable=False),
        StructField("inventoryCode", StringType(), nullable=False),
        StructField("itemType", StringType(), nullable=False),
        StructField("availableCount", IntegerType(), nullable=False),
        StructField("outOfOrderCount", IntegerType(), nullable=False),
        StructField("physicalCount", IntegerType(), nullable=False),
        StructField("soldCount", IntegerType(), nullable=False),
        StructField("groupBlockCount", IntegerType(), nullable=False),
        StructField("groupPickUpCount", IntegerType(), nullable=False),
        StructField("overbookingLimit", IntegerType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "inventoryAvailableCount": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("hotel_id", StringType(), nullable=False),
        StructField("startDate", DateType(), nullable=False),
        StructField("endDate", DateType(), nullable=False),
        StructField("inventoryCode", StringType(), nullable=False),
        StructField("itemType", StringType(), nullable=False),
        StructField("quantity", IntegerType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "inventoryOutOfOrderCount": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("hotel_id", StringType(), nullable=False),
        StructField("startDate", DateType(), nullable=False),
        StructField("endDate", DateType(), nullable=False),
        StructField("inventoryCode", StringType(), nullable=False),
        StructField("itemType", StringType(), nullable=False),
        StructField("quantity", IntegerType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "inventoryPhysicalCount": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("hotel_id", StringType(), nullable=False),
        StructField("startDate", DateType(), nullable=False),
        StructField("endDate", DateType(), nullable=False),
        StructField("inventoryCode", StringType(), nullable=False),
        StructField("itemType", StringType(), nullable=False),
        StructField("quantity", IntegerType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "housekeepingStatus": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("eventInfo_name", StringType(), nullable=True),
        StructField("eventInfo_version", StringType(), nullable=False),
        StructField("hotel_id", StringType(), nullable=False),
        StructField("roomNumber", StringType(), nullable=False),
        StructField("currentRoomCondition", StringType(), nullable=False),
        StructField("previousRoomCondition", StringType(), nullable=False),
        StructField("roomStatusUpdateTimestamp", TimestampType(), nullable=False),
        StructField("roomStatusUpdateOperatorId", StringType(), nullable=False),
        StructField("checkInReady", BooleanType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "housekeepingStatusConfirmationNumberLink": StructType([
        StructField("house_keeping_status_id", StringType(), nullable=False),
        StructField("confirmation_number_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "folioItem": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("transactionTimestamp", TimestampType(), nullable=True),
        StructField("transactionBusinessDate", DateType(), nullable=True),
        StructField("transactionDescription", StringType(), nullable=True),
        StructField("transactionComments", StringType(), nullable=True),
        StructField("transactionAmount", DecimalType(12, 2), nullable=True),
        StructField("transactionCode", StringType(), nullable=True),
        StructField("taxItem", BooleanType(), nullable=True),
        StructField("transactionType", StringType(), nullable=True),
        StructField("isHotelViewOnly", BooleanType(), nullable=True),
        StructField("folioId", StringType(), nullable=True),
        StructField("fromAccountTransfer_account", StringType(), nullable=True),
        StructField("fromAccountTransfer_accountType", StringType(), nullable=True),
        StructField("toAccountTransfer_account", StringType(), nullable=True),
        StructField("toAccountTransfer_accountType", StringType(), nullable=True),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "groupFolioItemLink": StructType([
        StructField("group_id", StringType(), nullable=False),
        StructField("folio_item_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "cleaningSchedule": StructType([
        StructField("id", StringType(), nullable=False),
        StructField("startDate", DateType(), nullable=False),
        StructField("endDate", DateType(), nullable=False),
        StructField("cleaningType", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
    
    "reservationCleaningScheduleLink": StructType([
        StructField("reservation_id", StringType(), nullable=False),
        StructField("cleaning_schedule_id", StringType(), nullable=False),
        StructField("received_at", TimestampType(), nullable=False),
        StructField("source_id", LongType(), nullable=False),
    ]),
}


def get_table_schema(table_name: str) -> StructType:
    """
    Get PySpark schema for a given table name in O(1) time complexity.
    
    Args:
        table_name: Name of the table (case-sensitive)
        
    Returns:
        StructType: PySpark schema for the requested table
        
    Raises:
        KeyError: If the table name is not found in the schemas dictionary
        
    Example:
        >>> schema = get_table_schema("hotel")
        >>> df = spark.createDataFrame(data, schema=schema)
    """
    if table_name not in SCHEMAS:
        available_tables = ", ".join(sorted(SCHEMAS.keys()))
        raise KeyError(
            f"Table '{table_name}' not found. Available tables: {available_tables}"
        )
    return SCHEMAS[table_name]


def get_all_table_names() -> list[str]:
    """
    Get a list of all available table names.
    
    Returns:
        list[str]: Sorted list of all table names
    """
    return sorted(SCHEMAS.keys())
