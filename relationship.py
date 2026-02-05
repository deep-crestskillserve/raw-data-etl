def get_table_relationships(table_name):
    """
    Returns a dictionary of parent tables and the local columns that refer to them.
    Time Complexity: O(1)
    """
    # Pre-computed mapping based on the provided SQL schema
    # Format: { "child_table": { "parent_table": "local_fk_column" } }
    schema_map = {
        "group": {
            "hotel": "hotel_id",
            "company": "company_id",
            "travelAgent": "travelagent_id"
        },
        "reservation": {
            "hotel": "hotels_id",
            "reservedRoom": "reservedRoom", # Also checkedOutRoom_id refers here
            "travelAgent": "associations_travelAgent_id",
            "group": "associations_groups_id",
            "company": "associations_company_id",
            "bookingChannel": "sourceOfBusiness_externalSource_sourceId"
        },
        "reservationConfirmationNumberLink": {
            "reservation": "reservation_id",
            "confirmationNumber": "confirmation_number_id"
        },
        "groupConfirmationNumberLink": {
            "group": "group_id",
            "confirmationNumber": "confirmation_number_id"
        },
        "reservationStatusLink": {
            "reservation": "id",
            "reservationStatus": "status_id"
        },
        "customerLoyaltyRewardsMembershipLink": {
            "customer": "customer_id",
            "loyaltyRewardsMembership": "loyalty_rewards_membership_id"
        },
        "contactContactPurposeLink": {
            "contact": "contact_id",
            "contactPurpose": "contact_purpose_id"
        },
        "customerContactLink": {
            "customer": "customer_id",
            "contact": "contact_id"
        },
        "groupContactLink": {
            "group": "group_id",
            "contact": "contact_id"
        },
        "reservationCustomerLink": {
            "reservation": "id",
            "customer": "customer_id"
        },
        "reservationEmailAddressLink": {
            "reservation": "reservation_id",
            "contact": "email_address_id"
        },
        "reservationItemReservedInventoryLink": {
            "reservationItem": "reservation_item_id",
            "reservedInventory": "reserved_inventory_id"
        },
        "reservationItemReservedRateLink": {
            "reservationItem": "reservation_item_id",
            "reservedRate": "reserved_rate_id"
        },
        "reservationItemReservedRoomLink": {
            "reservationItem": "reservation_item_id",
            "reservedRoom": "reserved_room_id"
        },
        "reservationItemSpecialRequestLink": {
            "reservationItem": "reservation_item_id",
            "specialRequest": "special_request_id"
        },
        "reservationItemLink": {
            "reservation": "reservation_id",
            "reservationItem": "reservation_item_id"
        },
        "guaranteeCreditCardLink": {
            "guarantee": "guarantee_id",
            "creditCard": "credit_card_id"
        },
        "reservationGuaranteeLink": {
            "reservation": "reservation_id",
            "guarantee": "guarantee_id"
        },
        "paymentMethodCreditCardLink": {
            "paymentMethod": "payment_method_id",
            "creditCard": "credit_card_id"
        },
        "reservationPaymentMethodLink": {
            "reservation": "reservation_id",
            "paymentMethod": "payment_method_id"
        },
        "reservationLinkedReservationLink": {
            "reservation": "reservation_id",
            "linkedReservation": "linked_reservation_id"
        },
        "reservationFolioSummaryLink": {
            "reservation": "reservation_id",
            "folioSummary": "folio_summary_id"
        },
        "groupFolioSummaryLink": {
            "group": "group_id",
            "folioSummary": "folio_summary_id"
        },
        "groupAccountStatusLink": {
            "group": "group_id",
            "accountStatus": "account_status_id"
        },
        "groupGroupBlockLink": {
            "group": "group_id",
            "groupBlock": "group_block_id"
        },
        "groupGroupRateLink": {
            "group": "group_id",
            "groupRate": "group_rate_id"
        },
        "groupPlannerCreditCardLink": {
            "group": "group_id",
            "creditCard": "credit_card_id"
        },
        "groupGuestCreditCardLink": {
            "group": "group_id",
            "creditCard": "credit_card_id"
        },
        "groupPaymentMethodLink": {
            "group": "group_id",
            "paymentMethod": "payment_method_id"
        },
        "groupLoyaltyRewardsMembershipLink": {
            "group": "group_id",
            "loyaltyRewardsMembership": "loyalty_rewards_membership_id"
        },
        "rate": {
            "hotel": "hotel_id"
        },
        "rateRatePlanCodeLink": {
            "rate": "rate_id",
            "ratePlanCode": "rate_plan_code_id"
        },
        "rateAmountOffRateLink": {
            "rate": "rate_id",
            "amountOffRate": "amount_off_rate_id"
        },
        "ratePercentOffRateLink": {
            "rate": "rate_id",
            "percentOffRate": "percent_off_rate_id"
        },
        "rateAmountAboveRateLink": {
            "rate": "rate_id",
            "amountAboveRate": "amount_above_rate_id"
        },
        "ratePercentAboveRateLink": {
            "rate": "rate_id",
            "percentAboveRate": "percent_above_rate_id"
        },
        "rateAmountFixedRateLink": {
            "rate": "rate_id",
            "amountFixedRate": "amount_fixed_rate_id"
        },
        "rateExtraChargeLink": {
            "rate": "rate_id",
            "extraCharge": "extra_charge_id"
        },
        "inventoryBatch": { "hotel": "hotel_id" },
        "inventoryAvailableCount": { "hotel": "hotel_id" },
        "inventoryOutOfOrderCount": { "hotel": "hotel_id" },
        "inventoryPhysicalCount": { "hotel": "hotel_id" },
        "housekeepingStatus": { "hotel": "hotel_id" },
        "housekeepingStatusConfirmationNumberLink": {
            "housekeepingStatus": "house_keeping_status_id",
            "confirmationNumber": "confirmation_number_id"
        },
        "groupFolioItemLink": {
            "group": "group_id",
            "folioItem": "folio_item_id"
        },
        "reservationCleaningScheduleLink": {
            "reservation": "reservation_id",
            "cleaningSchedule": "cleaning_schedule_id"
        }
    }

    return schema_map.get(table_name, {})