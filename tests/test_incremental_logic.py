"""
test_incremental_logic.py
Comprehensive testing suite for AWS Glue Job Bookmarks, Incremental Processing,
Dead-Letter Quarantining, and Idempotency Verification.

Tests all core scenarios:
- Test 1: Upload first CSV (Day 1) -> Verify all records processed and partitioned.
- Test 2: Upload second CSV (Day 2) -> Verify ONLY incremental records processed.
- Test 3: Run without new files -> Verify 0 records processed and no-op.
- Test 4: Upload file with invalid/corrupt records -> Verify quality checks and quarantine.
- Test 7: Simulate job failure -> Verify bookmark rollback and recovery state.
- Test 8: Rerun failed/retried batch -> Verify idempotent partition overwrite (no duplicate records).
"""

import os
import sys
import shutil
import tempfile
import csv
from datetime import datetime
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.generate_data import generate_dataset, generate_standard_pipeline_batches
from scripts.common_utils import compute_record_hash, parse_s3_uri
from scripts.data_validation import separate_valid_and_invalid_records


class MockGlueBookmarkState:
    """
    Simulates AWS Glue Job Bookmark state management for S3 CSV sources.
    Maintains a catalog of processed file paths, hashes, and last-modified times.
    """
    def __init__(self):
        self.committed_files = set()
        self.staging_files = set()

    def discover_new_files(self, source_dir: str):
        """Scans directory and returns only files not yet committed in bookmark."""
        all_files = [
            os.path.join(source_dir, f)
            for f in os.listdir(source_dir)
            if f.endswith(".csv")
        ]
        new_files = [f for f in all_files if f not in self.committed_files]
        self.staging_files = set(new_files)
        return sorted(new_files)

    def commit(self):
        """Commits staging files to bookmark state upon successful job completion."""
        self.committed_files.update(self.staging_files)
        self.staging_files.clear()

    def rollback(self):
        """Rolls back bookmark staging state if job fails before commit."""
        self.staging_files.clear()


class LocalIncrementalPipelineRunner:
    """
    Simulates the Glue ETL Job execution locally, applying transformations,
    partitioning, dead-letter quarantine, and dynamic partition overwrite.
    """
    def __init__(self, raw_dir: str, processed_dir: str, invalid_dir: str):
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        self.invalid_dir = invalid_dir
        self.bookmark = MockGlueBookmarkState()

    def run_etl_job(self, simulate_failure_before_commit: bool = False):
        new_files = self.bookmark.discover_new_files(self.raw_dir)
        if not new_files:
            return {
                "status": "NO_NEW_DATA",
                "files_read": 0,
                "valid_processed": 0,
                "invalid_quarantined": 0
            }

        raw_records = []
        for file_path in new_files:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    raw_records.append(row)

        valid_records, invalid_records, metrics = separate_valid_and_invalid_records(raw_records)

        # Write valid records with dynamic partition overwrite (by year/month/day)
        partitions_written = set()
        for rec in valid_records:
            b_dt = datetime.strptime(rec["booking_date"].split()[0], "%Y-%m-%d")
            part_path = os.path.join(
                self.processed_dir,
                f"year={b_dt.year:04d}",
                f"month={b_dt.month:02d}",
                f"day={b_dt.day:02d}"
            )
            os.makedirs(part_path, exist_ok=True)
            part_file = os.path.join(part_path, "processed_data.csv")

            # Check if this partition hasn't been initialized in this run (simulate partition overwrite)
            if part_path not in partitions_written:
                # In dynamic partition overwrite, existing data in this partition is replaced
                if os.path.exists(part_file):
                    os.remove(part_file)
                partitions_written.add(part_path)

            file_exists = os.path.exists(part_file)
            with open(part_file, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f, 
                    fieldnames=list(rec.keys()) + ["amount_usd", "record_hash"]
                )
                if not file_exists:
                    writer.writeheader()
                rec_with_hash = rec.copy()
                rec_with_hash["amount_usd"] = float(rec["amount"]) * 1.0  # Normalized
                rec_with_hash["record_hash"] = compute_record_hash(
                    rec["booking_id"], rec["customer_id"], rec["amount"], rec["booking_status"]
                )
                writer.writerow(rec_with_hash)

        # Write invalid records to quarantine directory
        if invalid_records:
            os.makedirs(self.invalid_dir, exist_ok=True)
            invalid_file = os.path.join(self.invalid_dir, "quarantine_records.csv")
            file_exists = os.path.exists(invalid_file)
            with open(invalid_file, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=list(invalid_records[0].keys()))
                if not file_exists:
                    writer.writeheader()
                writer.writerows(invalid_records)

        # Simulate job failure condition prior to bookmark commit
        if simulate_failure_before_commit:
            self.bookmark.rollback()
            raise RuntimeError("SIMULATED_GLUE_JOB_FAILURE: S3 write error before bookmark commit")

        # Successful completion commits bookmark
        self.bookmark.commit()

        return {
            "status": "SUCCESS",
            "files_read": len(new_files),
            "valid_processed": len(valid_records),
            "invalid_quarantined": len(invalid_records)
        }


@pytest.fixture
def pipeline_env(tmp_path):
    raw_dir = tmp_path / "raw-data"
    processed_dir = tmp_path / "processed"
    invalid_dir = tmp_path / "invalid"
    os.makedirs(raw_dir)
    os.makedirs(processed_dir)
    os.makedirs(invalid_dir)

    runner = LocalIncrementalPipelineRunner(str(raw_dir), str(processed_dir), str(invalid_dir))
    return runner, str(raw_dir), str(processed_dir), str(invalid_dir)


def test_scenario_1_initial_batch_ingestion(pipeline_env):
    """Test 1: Upload first CSV file (Day 1) and verify records are processed."""
    runner, raw_dir, processed_dir, _ = pipeline_env

    # 1. Generate Day 1 batch (100 clean records)
    d1_file = os.path.join(raw_dir, "bookings_2026-11-25.csv")
    generate_dataset("2026-11-25", 100, d1_file, include_dirty=False, seq_start=1000, seed=101)

    # 2. Run ETL job
    res = runner.run_etl_job()
    assert res["status"] == "SUCCESS"
    assert res["files_read"] == 1
    assert res["valid_processed"] == 100
    assert res["invalid_quarantined"] == 0

    # 3. Verify partitioned output exists
    part_file = os.path.join(processed_dir, "year=2026", "month=11", "day=25", "processed_data.csv")
    assert os.path.exists(part_file)

    with open(part_file, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        assert len(rows) == 100


def test_scenario_2_incremental_new_file_only(pipeline_env):
    """Test 2: Upload second CSV file (Day 2) and verify ONLY new files are processed."""
    runner, raw_dir, processed_dir, _ = pipeline_env

    # Run Day 1
    d1_file = os.path.join(raw_dir, "bookings_2026-11-25.csv")
    generate_dataset("2026-11-25", 100, d1_file, include_dirty=False, seq_start=1000, seed=101)
    runner.run_etl_job()

    # Upload Day 2 (50 new records)
    d2_file = os.path.join(raw_dir, "bookings_2026-11-26.csv")
    generate_dataset("2026-11-26", 50, d2_file, include_dirty=False, seq_start=2000, seed=202)

    # Run Day 2 ETL
    res2 = runner.run_etl_job()
    assert res2["status"] == "SUCCESS"
    assert res2["files_read"] == 1  # ONLY Day 2 file!
    assert res2["valid_processed"] == 50

    # Verify Day 2 partition created
    d2_part = os.path.join(processed_dir, "year=2026", "month=11", "day=26", "processed_data.csv")
    assert os.path.exists(d2_part)
    with open(d2_part, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        assert len(rows) == 50


def test_scenario_3_noop_when_no_new_files(pipeline_env):
    """Test 3: Run without uploading new files and verify no reprocessing."""
    runner, raw_dir, _, _ = pipeline_env

    # Run Day 1
    d1_file = os.path.join(raw_dir, "bookings_2026-11-25.csv")
    generate_dataset("2026-11-25", 50, d1_file, include_dirty=False, seq_start=1000)
    runner.run_etl_job()

    # Run again without any new uploads
    res_noop = runner.run_etl_job()
    assert res_noop["status"] == "NO_NEW_DATA"
    assert res_noop["files_read"] == 0
    assert res_noop["valid_processed"] == 0


def test_scenario_4_data_quality_and_quarantine(pipeline_env):
    """Test 4: Upload file containing corrupt/missing records and verify quarantine routing."""
    runner, raw_dir, processed_dir, invalid_dir = pipeline_env

    # Generate corrupt batch (30 records: 20 clean, 10 dirty/duplicate)
    d3_file = os.path.join(raw_dir, "bookings_2026-11-27_corrupt.csv")
    generate_dataset(
        "2026-11-27", 30, d3_file, include_dirty=True, dirty_ratio=0.30, duplicate_count=2, seed=303
    )

    res = runner.run_etl_job()
    assert res["status"] == "SUCCESS"
    assert res["invalid_quarantined"] > 0
    assert res["valid_processed"] > 0

    # Verify invalid records file was populated with quarantine details
    quarantine_file = os.path.join(invalid_dir, "quarantine_records.csv")
    assert os.path.exists(quarantine_file)
    with open(quarantine_file, "r", encoding="utf-8") as f:
        quarantine_rows = list(csv.DictReader(f))
        assert len(quarantine_rows) == res["invalid_quarantined"]
        for q in quarantine_rows:
            assert q["validation_error_codes"] != ""


def test_scenario_7_and_8_failure_recovery_and_idempotency(pipeline_env):
    """
    Test 7 & 8: Simulate job failure before bookmark commit, followed by rerun.
    Verify that rerun does not create duplicate records due to dynamic partition overwrite.
    """
    runner, raw_dir, processed_dir, _ = pipeline_env

    # Generate batch
    batch_file = os.path.join(raw_dir, "bookings_2026-11-25.csv")
    generate_dataset("2026-11-25", 40, batch_file, include_dirty=False, seq_start=1000, seed=404)

    # 1. Simulate failure on first attempt
    with pytest.raises(RuntimeError) as exc_info:
        runner.run_etl_job(simulate_failure_before_commit=True)
    assert "SIMULATED_GLUE_JOB_FAILURE" in str(exc_info.value)

    # Bookmark state was NOT committed due to rollback
    assert len(runner.bookmark.committed_files) == 0

    # 2. Retry the job (Rerun)
    res_retry = runner.run_etl_job(simulate_failure_before_commit=False)
    assert res_retry["status"] == "SUCCESS"
    assert res_retry["valid_processed"] == 40
    assert len(runner.bookmark.committed_files) == 1

    # 3. Check processed output row count: must be exactly 40 (no duplicates)
    part_file = os.path.join(processed_dir, "year=2026", "month=11", "day=25", "processed_data.csv")
    with open(part_file, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        assert len(rows) == 40, f"Expected 40 records after retry, found {len(rows)} (duplicate data detected!)"
