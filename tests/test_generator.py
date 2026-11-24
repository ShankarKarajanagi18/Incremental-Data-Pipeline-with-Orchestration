"""
test_generator.py
Unit tests for the synthetic dataset generator.
"""

import os
import sys
import csv
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.generate_data import (
    generate_clean_record,
    generate_dirty_record,
    generate_dataset,
    generate_standard_pipeline_batches,
    HEADER
)


def test_clean_record_structure():
    record = generate_clean_record(index=1, date_str="2026-11-25", seq_start=1000)
    for field in HEADER:
        assert field in record, f"Missing field {field}"
        assert record[field] != "", f"Field {field} should not be empty in clean record"
        
    assert record["booking_id"].startswith("BK-20261125-1001")
    assert record["customer_id"].startswith("CUST-")
    assert record["amount"] > 0
    assert record["booking_status"] in ["CONFIRMED", "CANCELLED", "PENDING", "COMPLETED"]


def test_dirty_record_anomalies():
    # Generate multiple dirty records and verify that anomalies exist
    records = [generate_dirty_record(i, "2026-11-27", 3000) for i in range(50)]
    has_empty = any(r["booking_id"] == "" or r["customer_id"] == "" for r in records)
    has_non_positive_amount = any(r["amount"] <= 0 for r in records)
    has_invalid_status_or_curr = any(
        r["booking_status"] == "UNKNOWN_CODE" or r["currency"] == "XYZ" for r in records
    )
    assert has_empty or has_non_positive_amount or has_invalid_status_or_curr


def test_dataset_generation_batch(tmp_path):
    out_csv = tmp_path / "test_batch.csv"
    records = generate_dataset(
        batch_date="2026-11-25",
        record_count=20,
        output_path=str(out_csv),
        include_dirty=False,
        duplicate_count=2
    )
    assert os.path.exists(out_csv)
    # 20 + 2 duplicates = 22 rows
    assert len(records) == 22

    with open(out_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 22
        assert reader.fieldnames == HEADER
