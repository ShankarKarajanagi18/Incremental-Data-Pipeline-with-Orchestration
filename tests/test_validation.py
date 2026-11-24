"""
test_validation.py
Unit tests for data quality validation, defect detection, and dead-letter quarantining.
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.data_validation import (
    validate_booking_record,
    separate_valid_and_invalid_records,
    ValidationErrorCodes
)


def test_valid_record_passes_validation():
    valid_record = {
        "booking_id": "BK-20261125-1001",
        "customer_id": "CUST-4812",
        "destination": "Tokyo",
        "booking_date": "2026-11-25 10:00:00",
        "travel_date": "2026-12-15",
        "amount": 1250.00,
        "currency": "USD",
        "booking_status": "CONFIRMED",
        "payment_method": "CREDIT_CARD"
    }
    is_valid, codes, msgs = validate_booking_record(valid_record)
    assert is_valid is True
    assert len(codes) == 0
    assert len(msgs) == 0


def test_missing_or_malformed_booking_id():
    # Empty booking ID
    rec_empty = {
        "booking_id": "",
        "customer_id": "CUST-1001",
        "destination": "Paris",
        "booking_date": "2026-11-25 10:00:00",
        "travel_date": "2026-12-01",
        "amount": 500.00,
        "currency": "EUR",
        "booking_status": "CONFIRMED",
        "payment_method": "CREDIT_CARD"
    }
    is_valid, codes, _ = validate_booking_record(rec_empty)
    assert is_valid is False
    assert ValidationErrorCodes.MISSING_BOOKING_ID in codes

    # Malformed booking ID
    rec_malformed = rec_empty.copy()
    rec_malformed["booking_id"] = "INVALID_ID_123"
    is_valid, codes, _ = validate_booking_record(rec_malformed)
    assert is_valid is False
    assert ValidationErrorCodes.MALFORMED_BOOKING_ID in codes


def test_invalid_negative_or_zero_amount():
    rec_neg = {
        "booking_id": "BK-20261125-1002",
        "customer_id": "CUST-1002",
        "destination": "London",
        "booking_date": "2026-11-25 10:00:00",
        "travel_date": "2026-12-01",
        "amount": -50.0,
        "currency": "GBP",
        "booking_status": "CONFIRMED",
        "payment_method": "CREDIT_CARD"
    }
    is_valid, codes, _ = validate_booking_record(rec_neg)
    assert is_valid is False
    assert ValidationErrorCodes.INVALID_AMOUNT in codes

    rec_zero = rec_neg.copy()
    rec_zero["amount"] = 0.0
    is_valid, codes, _ = validate_booking_record(rec_zero)
    assert is_valid is False
    assert ValidationErrorCodes.INVALID_AMOUNT in codes


def test_chronology_violation_travel_before_booking():
    rec = {
        "booking_id": "BK-20261125-1003",
        "customer_id": "CUST-1003",
        "destination": "Dubai",
        "booking_date": "2026-11-25 10:00:00",
        "travel_date": "2026-11-20",  # 5 days before booking
        "amount": 1200.0,
        "currency": "USD",
        "booking_status": "CONFIRMED",
        "payment_method": "PAYPAL"
    }
    is_valid, codes, _ = validate_booking_record(rec)
    assert is_valid is False
    assert ValidationErrorCodes.CHRONOLOGY_VIOLATION in codes


def test_unsupported_currency_and_status():
    rec = {
        "booking_id": "BK-20261125-1004",
        "customer_id": "CUST-1004",
        "destination": "Sydney",
        "booking_date": "2026-11-25 10:00:00",
        "travel_date": "2026-12-25",
        "amount": 2200.0,
        "currency": "XYZ_BAD",
        "booking_status": "FAKE_STATUS",
        "payment_method": "CREDIT_CARD"
    }
    is_valid, codes, _ = validate_booking_record(rec)
    assert is_valid is False
    assert ValidationErrorCodes.INVALID_CURRENCY in codes
    assert ValidationErrorCodes.INVALID_STATUS in codes


def test_batch_separation_and_deduplication():
    records = [
        # Valid 1
        {
            "booking_id": "BK-20261125-1010",
            "customer_id": "CUST-1010",
            "destination": "Rome",
            "booking_date": "2026-11-25 12:00:00",
            "travel_date": "2026-12-10",
            "amount": 800.0,
            "currency": "EUR",
            "booking_status": "CONFIRMED",
            "payment_method": "CREDIT_CARD"
        },
        # Duplicate of Valid 1
        {
            "booking_id": "BK-20261125-1010",
            "customer_id": "CUST-1010",
            "destination": "Rome",
            "booking_date": "2026-11-25 12:00:00",
            "travel_date": "2026-12-10",
            "amount": 800.0,
            "currency": "EUR",
            "booking_status": "CONFIRMED",
            "payment_method": "CREDIT_CARD"
        },
        # Invalid 1 (negative amount)
        {
            "booking_id": "BK-20261125-1011",
            "customer_id": "CUST-1011",
            "destination": "Tokyo",
            "booking_date": "2026-11-25 12:00:00",
            "travel_date": "2026-12-10",
            "amount": -100.0,
            "currency": "USD",
            "booking_status": "CONFIRMED",
            "payment_method": "CREDIT_CARD"
        }
    ]

    valid_recs, invalid_recs, metrics = separate_valid_and_invalid_records(records)
    assert len(valid_recs) == 1
    assert len(invalid_recs) == 2  # 1 duplicate + 1 negative amount
    assert metrics["total_records"] == 3
    assert metrics["valid_records"] == 1
    assert metrics["invalid_records"] == 2

    # Check that invalid records retain original payload and have quarantine metadata
    assert "validation_error_codes" in invalid_recs[0]
    assert "quarantine_timestamp" in invalid_recs[0]
