"""
generate_data.py
Synthetic Travel Booking Dataset Generator for AWS Glue Incremental Data Pipeline.

Generates realistic CSV datasets for travel bookings:
- Day 1: Baseline clean batch
- Day 2: Incremental new batch
- Day 3: Corrupted / edge-case batch with nulls, negative amounts, invalid dates, and duplicates
"""

import os
import sys
import csv
import random
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

DESTINATIONS = [
    "Tokyo", "Paris", "New York", "London", "Dubai", 
    "Singapore", "Sydney", "Rome", "Bangkok", "Cape Town",
    "Barcelona", "Toronto", "Honolulu", "Reykjavik", "Amsterdam"
]

CURRENCIES = ["USD", "EUR", "GBP", "CAD", "AUD", "JPY"]

STATUSES = ["CONFIRMED", "CANCELLED", "PENDING", "COMPLETED"]

PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER", "CRYPTO"]

HEADER = [
    "booking_id",
    "customer_id",
    "destination",
    "booking_date",
    "travel_date",
    "amount",
    "currency",
    "booking_status",
    "payment_method"
]


def generate_clean_record(
    index: int, 
    date_str: str, 
    seq_start: int = 1000,
    rng: Optional[random.Random] = None
) -> Dict[str, Any]:
    """Generates a single valid, production-grade travel booking record."""
    r = rng if rng else random
    booking_id = f"BK-{date_str.replace('-', '')}-{seq_start + index}"
    customer_id = f"CUST-{r.randint(1000, 9999)}"
    destination = r.choice(DESTINATIONS)
    
    # Booking time within the given date
    hour = r.randint(0, 23)
    minute = r.randint(0, 59)
    second = r.randint(0, 59)
    booking_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(
        hour=hour, minute=minute, second=second
    )
    booking_date = booking_dt.strftime("%Y-%m-%d %H:%M:%S")
    
    # Travel date is 3 to 180 days in the future
    lead_days = r.randint(3, 180)
    travel_dt = booking_dt + timedelta(days=lead_days)
    travel_date = travel_dt.strftime("%Y-%m-%d")
    
    amount = round(r.uniform(150.0, 4800.0), 2)
    currency = r.choice(CURRENCIES)
    booking_status = r.choices(
        STATUSES, weights=[0.70, 0.10, 0.15, 0.05], k=1
    )[0]
    payment_method = r.choice(PAYMENT_METHODS)
    
    return {
        "booking_id": booking_id,
        "customer_id": customer_id,
        "destination": destination,
        "booking_date": booking_date,
        "travel_date": travel_date,
        "amount": amount,
        "currency": currency,
        "booking_status": booking_status,
        "payment_method": payment_method
    }


def generate_dirty_record(
    index: int, 
    date_str: str, 
    seq_start: int = 5000,
    rng: Optional[random.Random] = None
) -> Dict[str, Any]:
    """
    Generates a record containing intentional anomalies to test data validation:
    1. Null / empty booking_id
    2. Null / empty customer_id
    3. Negative or zero amount
    4. Malformed date format
    5. travel_date prior to booking_date
    6. Invalid status / currency
    """
    r = rng if rng else random
    record = generate_clean_record(index, date_str, seq_start, r)
    
    defect_type = r.choice([
        "null_booking_id",
        "null_customer_id",
        "negative_amount",
        "zero_amount",
        "invalid_travel_date",
        "past_travel_date",
        "malformed_booking_date",
        "invalid_currency",
        "invalid_status"
    ])
    
    if defect_type == "null_booking_id":
        record["booking_id"] = ""
    elif defect_type == "null_customer_id":
        record["customer_id"] = ""
    elif defect_type == "negative_amount":
        record["amount"] = -1 * round(r.uniform(50.0, 1200.0), 2)
    elif defect_type == "zero_amount":
        record["amount"] = 0.00
    elif defect_type == "invalid_travel_date":
        record["travel_date"] = "INVALID_DATE"
    elif defect_type == "past_travel_date":
        # Travel date before booking date
        b_dt = datetime.strptime(date_str, "%Y-%m-%d")
        record["travel_date"] = (b_dt - timedelta(days=10)).strftime("%Y-%m-%d")
    elif defect_type == "malformed_booking_date":
        record["booking_date"] = "25/11/2026 14:00"  # Non-standard format
    elif defect_type == "invalid_currency":
        record["currency"] = "XYZ"
    elif defect_type == "invalid_status":
        record["booking_status"] = "UNKNOWN_CODE"
        
    return record


def generate_dataset(
    batch_date: str,
    record_count: int,
    output_path: str,
    include_dirty: bool = False,
    dirty_ratio: float = 0.15,
    duplicate_count: int = 0,
    seq_start: int = 1000,
    seed: Optional[int] = 42
) -> List[Dict[str, Any]]:
    """Generates and writes a CSV dataset of travel bookings."""
    rng = random.Random(seed)
    records: List[Dict[str, Any]] = []
    
    num_dirty = int(record_count * dirty_ratio) if include_dirty else 0
    num_clean = record_count - num_dirty
    
    # Clean records
    for i in range(num_clean):
        records.append(generate_clean_record(i, batch_date, seq_start, rng))
        
    # Dirty records
    for i in range(num_dirty):
        records.append(generate_dirty_record(num_clean + i, batch_date, seq_start, rng))
        
    # Duplicate records (cloning existing records to test deduplication)
    for i in range(duplicate_count):
        if records:
            dupe = records[i % len(records)].copy()
            records.append(dupe)
            
    # Shuffle so anomalies are distributed throughout the file
    rng.shuffle(records)
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    
    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        writer.writerows(records)
        
    return records


def generate_standard_pipeline_batches(base_dir: str) -> Dict[str, str]:
    """Generates standard Day 1, Day 2, and Day 3 test batches for pipeline verification."""
    output_files = {}
    
    # Day 1: Baseline Clean Batch (100 records)
    d1_file = os.path.join(base_dir, "bookings_2026-11-25.csv")
    generate_dataset(
        batch_date="2026-11-25",
        record_count=100,
        output_path=d1_file,
        include_dirty=False,
        seq_start=1000,
        seed=101
    )
    output_files["day_1"] = d1_file
    
    # Day 2: Incremental Clean Batch (50 records)
    d2_file = os.path.join(base_dir, "bookings_2026-11-26.csv")
    generate_dataset(
        batch_date="2026-11-26",
        record_count=50,
        output_path=d2_file,
        include_dirty=False,
        seq_start=2000,
        seed=202
    )
    output_files["day_2"] = d2_file
    
    # Day 3: Corrupt / Edge-Case Batch (40 records with duplicates and defects)
    d3_file = os.path.join(base_dir, "bookings_2026-11-27_corrupt.csv")
    generate_dataset(
        batch_date="2026-11-27",
        record_count=40,
        output_path=d3_file,
        include_dirty=True,
        dirty_ratio=0.30,
        duplicate_count=4,
        seq_start=3000,
        seed=303
    )
    output_files["day_3"] = d3_file
    
    return output_files


def main():
    parser = argparse.ArgumentParser(description="Generate Synthetic Travel Booking Datasets")
    parser.add_argument("--batch-date", type=str, default="2026-11-25", help="Batch date (YYYY-MM-DD)")
    parser.add_argument("--records", type=int, default=100, help="Number of records to generate")
    parser.add_argument("--output", type=str, default=None, help="Output CSV path")
    parser.add_argument("--include-dirty", action="store_true", help="Include invalid records")
    parser.add_argument("--dirty-ratio", type=float, default=0.20, help="Ratio of dirty records")
    parser.add_argument("--duplicates", type=int, default=0, help="Number of duplicate records to inject")
    parser.add_argument("--generate-all-test-batches", action="store_true", help="Generate Day 1, 2, and 3 standard batches")
    parser.add_argument("--output-dir", type=str, default="data/sample_bookings", help="Output directory")

    args = parser.parse_args()
    
    if args.generate_all_test_batches:
        print(f"Generating all standard test batches in {args.output_dir}...")
        files = generate_standard_pipeline_batches(args.output_dir)
        for name, path in files.items():
            print(f"  [+] Generated {name}: {path}")
    else:
        out_path = args.output or os.path.join(args.output_dir, f"bookings_{args.batch_date}.csv")
        recs = generate_dataset(
            batch_date=args.batch_date,
            record_count=args.records,
            output_path=out_path,
            include_dirty=args.include_dirty,
            dirty_ratio=args.dirty_ratio,
            duplicate_count=args.duplicates
        )
        print(f"Generated {len(recs)} records to {out_path}")


if __name__ == "__main__":
    main()
