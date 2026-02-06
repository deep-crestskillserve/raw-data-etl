schema_map = {
    "group": {
        "hotel": {"local_column": "hotel_id", "parent_column": "id"},
        "company": {"local_column": "company_id", "parent_column": "id"},
        "travelAgent": {"local_column": "travelagent_id", "parent_column": "id"}
    },
    "reservation": {
        "hotel": {"local_column": "hotels_id", "parent_column": "id"},
        "reservedRoom": {"local_column": "reservedRoom", "parent_column": "id"},
        # "reservedRoom_2": {"local_column": "checkedOutRoom_id", "parent_column": "id"},
        "travelAgent": {"local_column": "associations_travelAgent_id", "parent_column": "id"},
        # "group": {"local_column": "associations_groups_id", "parent_column": "id"},
        # "group_2": {"local_column": "checkOut_group_id", "parent_column": "id"},
        "company": {"local_column": "associations_company_id", "parent_column": "id"},
        # "bookingChannel": {"local_column": "sourceOfBusiness_externalSource_sourceId", "parent_column": "internalCode"}
    },
    "reservationConfirmationNumberLink": {
        "reservation": {"local_column": "reservation_id", "parent_column": "id"},
        "confirmationNumber": {"local_column": "confirmation_number_id", "parent_column": "id"}
    },
    "groupConfirmationNumberLink": {
        "group": {"local_column": "group_id", "parent_column": "id"},
        "confirmationNumber": {"local_column": "confirmation_number_id", "parent_column": "id"}
    },
    "reservationStatusLink": {
        "reservation": {"local_column": "id", "parent_column": "id"},
        "reservationStatus": {"local_column": "status_id", "parent_column": "id"}
    },
    "customerLoyaltyRewardsMembershipLink": {
        "customer": {"local_column": "customer_id", "parent_column": "id"},
        "loyaltyRewardsMembership": {"local_column": "loyalty_rewards_membership_id", "parent_column": "id"}
    },
    "contactContactPurposeLink": {
        "contact": {"local_column": "contact_id", "parent_column": "id"},
        "contactPurpose": {"local_column": "contact_purpose_id", "parent_column": "id"}
    },
    "customerContactLink": {
        "customer": {"local_column": "customer_id", "parent_column": "id"},
        "contact": {"local_column": "contact_id", "parent_column": "id"}
    },
    "groupContactLink": {
        "group": {"local_column": "group_id", "parent_column": "id"},
        "contact": {"local_column": "contact_id", "parent_column": "id"}
    },
    "reservationCustomerLink": {
        "reservation": {"local_column": "id", "parent_column": "id"},
        "customer": {"local_column": "customer_id", "parent_column": "id"}
    },
    "reservationEmailAddressLink": {
        "reservation": {"local_column": "reservation_id", "parent_column": "id"},
        "contact": {"local_column": "email_address_id", "parent_column": "id"}
    },
    "reservationItemReservedInventoryLink": {
        "reservationItem": {"local_column": "reservation_item_id", "parent_column": "id"},
        "reservedInventory": {"local_column": "reserved_inventory_id", "parent_column": "id"}
    },
    "reservationItemReservedRateLink": {
        "reservationItem": {"local_column": "reservation_item_id", "parent_column": "id"},
        "reservedRate": {"local_column": "reserved_rate_id", "parent_column": "id"}
    },
    "reservationItemReservedRoomLink": {
        "reservationItem": {"local_column": "reservation_item_id", "parent_column": "id"},
        "reservedRoom": {"local_column": "reserved_room_id", "parent_column": "id"}
    },
    "reservationItemSpecialRequestLink": {
        "reservationItem": {"local_column": "reservation_item_id", "parent_column": "id"},
        "specialRequest": {"local_column": "special_request_id", "parent_column": "id"}
    },
    "reservationItemLink": {
        "reservation": {"local_column": "reservation_id", "parent_column": "id"},
        "reservationItem": {"local_column": "reservation_item_id", "parent_column": "id"}
    },
    "guaranteeCreditCardLink": {
        "guarantee": {"local_column": "guarantee_id", "parent_column": "id"},
        "creditCard": {"local_column": "credit_card_id", "parent_column": "id"}
    },
    "reservationGuaranteeLink": {
        "reservation": {"local_column": "reservation_id", "parent_column": "id"},
        "guarantee": {"local_column": "guarantee_id", "parent_column": "id"}
    },
    "paymentMethodCreditCardLink": {
        "paymentMethod": {"local_column": "payment_method_id", "parent_column": "id"},
        "creditCard": {"local_column": "credit_card_id", "parent_column": "id"}
    },
    "reservationPaymentMethodLink": {
        "reservation": {"local_column": "reservation_id", "parent_column": "id"},
        "paymentMethod": {"local_column": "payment_method_id", "parent_column": "id"}
    },
    "reservationLinkedReservationLink": {
        "reservation": {"local_column": "reservation_id", "parent_column": "id"},
        "linkedReservation": {"local_column": "linked_reservation_id", "parent_column": "id"}
    },
    "reservationFolioSummaryLink": {
        "reservation": {"local_column": "reservation_id", "parent_column": "id"},
        "folioSummary": {"local_column": "folio_summary_id", "parent_column": "id"}
    },
    "groupFolioSummaryLink": {
        "group": {"local_column": "group_id", "parent_column": "id"},
        "folioSummary": {"local_column": "folio_summary_id", "parent_column": "id"}
    },
    "groupAccountStatusLink": {
        "group": {"local_column": "group_id", "parent_column": "id"},
        "accountStatus": {"local_column": "account_status_id", "parent_column": "id"}
    },
    "groupGroupBlockLink": {
        "group": {"local_column": "group_id", "parent_column": "id"},
        "groupBlock": {"local_column": "group_block_id", "parent_column": "id"}
    },
    "groupGroupRateLink": {
        "group": {"local_column": "group_id", "parent_column": "id"},
        "groupRate": {"local_column": "group_rate_id", "parent_column": "id"}
    },
    "groupPlannerCreditCardLink": {
        "group": {"local_column": "group_id", "parent_column": "id"},
        "creditCard": {"local_column": "credit_card_id", "parent_column": "id"}
    },
    "groupGuestCreditCardLink": {
        "group": {"local_column": "group_id", "parent_column": "id"},
        "creditCard": {"local_column": "credit_card_id", "parent_column": "id"}
    },
    "groupPaymentMethodLink": {
        "group": {"local_column": "group_id", "parent_column": "id"},
        "paymentMethod": {"local_column": "payment_method_id", "parent_column": "id"}
    },
    "groupLoyaltyRewardsMembershipLink": {
        "group": {"local_column": "group_id", "parent_column": "id"},
        "loyaltyRewardsMembership": {"local_column": "loyalty_rewards_membership_id", "parent_column": "id"}
    },
    "rate": {
        "hotel": {"local_column": "hotel_id", "parent_column": "id"}
    },
    "rateRatePlanCodeLink": {
        "rate": {"local_column": "rate_id", "parent_column": "id"},
        "ratePlanCode": {"local_column": "rate_plan_code_id", "parent_column": "id"}
    },
    "rateAmountOffRateLink": {
        "rate": {"local_column": "rate_id", "parent_column": "id"},
        "amountOffRate": {"local_column": "amount_off_rate_id", "parent_column": "id"}
    },
    "ratePercentOffRateLink": {
        "rate": {"local_column": "rate_id", "parent_column": "id"},
        "percentOffRate": {"local_column": "percent_off_rate_id", "parent_column": "id"}
    },
    "rateAmountAboveRateLink": {
        "rate": {"local_column": "rate_id", "parent_column": "id"},
        "amountAboveRate": {"local_column": "amount_above_rate_id", "parent_column": "id"}
    },
    "ratePercentAboveRateLink": {
        "rate": {"local_column": "rate_id", "parent_column": "id"},
        "percentAboveRate": {"local_column": "percent_above_rate_id", "parent_column": "id"}
    },
    "rateAmountFixedRateLink": {
        "rate": {"local_column": "rate_id", "parent_column": "id"},
        "amountFixedRate": {"local_column": "amount_fixed_rate_id", "parent_column": "id"}
    },
    "rateExtraChargeLink": {
        "rate": {"local_column": "rate_id", "parent_column": "id"},
        "extraCharge": {"local_column": "extra_charge_id", "parent_column": "id"}
    },
    "inventoryBatch": {
        "hotel": {"local_column": "hotel_id", "parent_column": "id"}
    },
    "inventoryAvailableCount": {
        "hotel": {"local_column": "hotel_id", "parent_column": "id"}
    },
    "inventoryOutOfOrderCount": {
        "hotel": {"local_column": "hotel_id", "parent_column": "id"}
    },
    "inventoryPhysicalCount": {
        "hotel": {"local_column": "hotel_id", "parent_column": "id"}
    },
    "housekeepingStatus": {
        "hotel": {"local_column": "hotel_id", "parent_column": "id"}
    },
    "housekeepingStatusConfirmationNumberLink": {
        "housekeepingStatus": {"local_column": "house_keeping_status_id", "parent_column": "id"},
        "confirmationNumber": {"local_column": "confirmation_number_id", "parent_column": "id"}
    },
    "groupFolioItemLink": {
        "group": {"local_column": "group_id", "parent_column": "id"},
        "folioItem": {"local_column": "folio_item_id", "parent_column": "id"}
    },
    "reservationCleaningScheduleLink": {
        "reservation": {"local_column": "reservation_id", "parent_column": "id"},
        "cleaningSchedule": {"local_column": "cleaning_schedule_id", "parent_column": "id"}
    }
}


def get_table_relationships(table_name):
    """
    Returns a dictionary of parent tables and the local columns that refer to them,
    along with the parent table's primary key column.
    Time Complexity: O(1)
    
    Returns format:
    {
        "parent_table": {
            "local_column": "local_fk_column_name",
            "parent_column": "parent_pk_column_name"
        }
    }
    Note: When a child table has multiple foreign keys to the same parent table,
    the key will be suffixed with a number (e.g., "reservedRoom_1", "reservedRoom_2").
    """
    # Pre-computed mapping based on the provided SQL schema
    # Format: { "child_table": { "parent_table": { "local_column": "fk_col", "parent_column": "pk_col" } } }
    
    return schema_map.get(table_name, {})

def table_exists_in_schema(table_name):
    return table_name in schema_map

# relationships = get_table_relationships("reservation")

# for parent_table, relation_info in relationships.items():
#     local_column = relation_info["local_column"]
#     parent_column = relation_info["parent_column"]

#     print("parent_table:", parent_table)
#     print("local_column:", local_column)
#     print("parent_column:", parent_column)
#     print("-" * 40)