"""
test_transformations.py
Unit tests for data transformations, currency conversions, hashing, and utility functions.
"""

import os
import sys
import pytest
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.common_utils import (
    compute_record_hash,
    parse_s3_uri,
    normalize_currency_amount,
    get_partition_path,
    EXCHANGE_RATES_TO_USD
)


def test_compute_record_hash_deterministic():
    hash1 = compute_record_hash("BK-20261125-1001", "CUST-1001", 1500.0, "CONFIRMED")
    hash2 = compute_record_hash("BK-20261125-1001", "CUST-1001", 1500.0, "CONFIRMED")
    hash3 = compute_record_hash("BK-20261125-1002", "CUST-1001", 1500.0, "CONFIRMED")

    assert hash1 == hash2, "Identical records must generate identical hashes"
    assert hash1 != hash3, "Different records must generate different hashes"
    assert len(hash1) == 64, "SHA-256 hash must be 64 characters hex"


def test_parse_s3_uri():
    bucket, prefix = parse_s3_uri("s3://my-test-bucket/raw-data/bookings/")
    assert bucket == "my-test-bucket"
    assert prefix == "raw-data/bookings/"

    b2, p2 = parse_s3_uri("s3a://analytics-bucket/processed/")
    assert b2 == "analytics-bucket"
    assert p2 == "processed/"


def test_normalize_currency_amount():
    # USD 1:1
    assert normalize_currency_amount(100.0, "USD") == 100.0
    # EUR
    assert normalize_currency_amount(100.0, "EUR") == round(100.0 * EXCHANGE_RATES_TO_USD["EUR"], 2)
    # JPY
    assert normalize_currency_amount(10000.0, "JPY") == round(10000.0 * EXCHANGE_RATES_TO_USD["JPY"], 2)
    # Unsupported currency returns None
    assert normalize_currency_amount(100.0, "UNKNOWN") is None


def test_get_partition_path():
    dt = datetime(2026, 11, 25, 15, 30, 0)
    path = get_partition_path(dt)
    assert path == "year=2026/month=11/day=25"
