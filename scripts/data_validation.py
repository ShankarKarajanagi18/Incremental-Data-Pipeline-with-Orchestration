"""
data_validation.py
Data Quality Rules Engine and Dead-Letter Routing for AWS Glue Incremental Pipeline.

Validates raw travel booking records against business rules:
- Mandatory non-null keys (booking_id, customer_id)
- Positive numeric amounts
- Chronological consistency (travel_date >= booking_date)
- Valid reference values (currency, booking_status)
- Batch-level duplicate detection

Routes valid records to processed storage and invalid records to dead-letter quarantine.
"""

import re
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple
try:
    from scripts.common_utils import (
        EXCHANGE_RATES_TO_USD,
        VALID_STATUSES,
        VALID_PAYMENT_METHODS,
        get_logger
    )
except (ImportError, ModuleNotFoundError):
    from common_utils import (
        EXCHANGE_RATES_TO_USD,
        VALID_STATUSES,
        VALID_PAYMENT_METHODS,
        get_logger
    )

logger = get_logger("data_validation")

BOOKING_ID_PATTERN = re.compile(r"^BK-\d{8}-\d{3,6}$")
CUSTOMER_ID_PATTERN = re.compile(r"^CUST-\d{3,6}$")


class ValidationErrorCodes:
    MISSING_BOOKING_ID = "ERR_MISSING_BOOKING_ID"
    MALFORMED_BOOKING_ID = "ERR_MALFORMED_BOOKING_ID"
    MISSING_CUSTOMER_ID = "ERR_MISSING_CUSTOMER_ID"
    MALFORMED_CUSTOMER_ID = "ERR_MALFORMED_CUSTOMER_ID"
    INVALID_AMOUNT = "ERR_INVALID_OR_NEGATIVE_AMOUNT"
    MALFORMED_BOOKING_DATE = "ERR_MALFORMED_BOOKING_DATE"
    MALFORMED_TRAVEL_DATE = "ERR_MALFORMED_TRAVEL_DATE"
    CHRONOLOGY_VIOLATION = "ERR_TRAVEL_BEFORE_BOOKING"
    INVALID_CURRENCY = "ERR_UNSUPPORTED_CURRENCY"
    INVALID_STATUS = "ERR_INVALID_STATUS"
    DUPLICATE_BOOKING_ID = "ERR_DUPLICATE_BOOKING_ID"


def validate_booking_record(record: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    """
    Validates a single booking record dictionary.
    Returns:
        (is_valid, error_codes, error_messages)
    """
    error_codes: List[str] = []
    error_messages: List[str] = []

    # 1. Booking ID Validation
    booking_id = str(record.get("booking_id") or "").strip()
    if not booking_id:
        error_codes.append(ValidationErrorCodes.MISSING_BOOKING_ID)
        error_messages.append("booking_id is null or empty")
    elif not BOOKING_ID_PATTERN.match(booking_id):
        error_codes.append(ValidationErrorCodes.MALFORMED_BOOKING_ID)
        error_messages.append(f"booking_id '{booking_id}' does not match pattern BK-YYYYMMDD-XXXX")

    # 2. Customer ID Validation
    customer_id = str(record.get("customer_id") or "").strip()
    if not customer_id:
        error_codes.append(ValidationErrorCodes.MISSING_CUSTOMER_ID)
        error_messages.append("customer_id is null or empty")
    elif not CUSTOMER_ID_PATTERN.match(customer_id):
        error_codes.append(ValidationErrorCodes.MALFORMED_CUSTOMER_ID)
        error_messages.append(f"customer_id '{customer_id}' does not match pattern CUST-XXXX")

    # 3. Amount Validation
    raw_amount = record.get("amount")
    try:
        amount_val = float(raw_amount)
        if amount_val <= 0.0:
            error_codes.append(ValidationErrorCodes.INVALID_AMOUNT)
            error_messages.append(f"amount must be strictly positive, found: {amount_val}")
    except (ValueError, TypeError):
        error_codes.append(ValidationErrorCodes.INVALID_AMOUNT)
        error_messages.append(f"amount '{raw_amount}' cannot be cast to numeric")

    # 4. Currency Validation
    currency = str(record.get("currency") or "").strip().upper()
    if currency not in EXCHANGE_RATES_TO_USD:
        error_codes.append(ValidationErrorCodes.INVALID_CURRENCY)
        error_messages.append(f"currency '{currency}' is not supported")

    # 5. Status Validation
    status = str(record.get("booking_status") or "").strip().upper()
    if status not in VALID_STATUSES:
        error_codes.append(ValidationErrorCodes.INVALID_STATUS)
        error_messages.append(f"booking_status '{status}' is invalid")

    # 6. Date Parsing and Chronology Check
    raw_b_date = str(record.get("booking_date") or "").strip()
    raw_t_date = str(record.get("travel_date") or "").strip()
    b_dt: Optional[datetime] = None
    t_dt: Optional[datetime] = None

    if not raw_b_date:
        error_codes.append(ValidationErrorCodes.MALFORMED_BOOKING_DATE)
        error_messages.append("booking_date is empty")
    else:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
            try:
                b_dt = datetime.strptime(raw_b_date, fmt)
                break
            except ValueError:
                pass
        if b_dt is None:
            error_codes.append(ValidationErrorCodes.MALFORMED_BOOKING_DATE)
            error_messages.append(f"booking_date '{raw_b_date}' is not standard ISO format")

    if not raw_t_date:
        error_codes.append(ValidationErrorCodes.MALFORMED_TRAVEL_DATE)
        error_messages.append("travel_date is empty")
    else:
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                t_dt = datetime.strptime(raw_t_date, fmt)
                break
            except ValueError:
                pass
        if t_dt is None:
            error_codes.append(ValidationErrorCodes.MALFORMED_TRAVEL_DATE)
            error_messages.append(f"travel_date '{raw_t_date}' is not standard YYYY-MM-DD")

    # If both dates are valid, verify travel_date >= booking_date (date portion)
    if b_dt and t_dt:
        if t_dt.date() < b_dt.date():
            error_codes.append(ValidationErrorCodes.CHRONOLOGY_VIOLATION)
            error_messages.append(f"travel_date ({t_dt.date()}) is before booking_date ({b_dt.date()})")

    is_valid = len(error_codes) == 0
    return is_valid, error_codes, error_messages


def separate_valid_and_invalid_records(
    records: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Partitions raw records into clean valid records and dead-letter invalid records.
    Performs in-batch deduplication on booking_id.
    """
    valid_records: List[Dict[str, Any]] = []
    invalid_records: List[Dict[str, Any]] = []
    seen_booking_ids = set()
    error_counts: Dict[str, int] = {}

    for record in records:
        is_valid, codes, msgs = validate_booking_record(record)
        b_id = str(record.get("booking_id") or "").strip()

        # Check for in-batch duplicate
        if is_valid and b_id:
            if b_id in seen_booking_ids:
                is_valid = False
                codes.append(ValidationErrorCodes.DUPLICATE_BOOKING_ID)
                msgs.append(f"Duplicate booking_id '{b_id}' within current batch")
            else:
                seen_booking_ids.add(b_id)

        if is_valid:
            valid_records.append(record)
        else:
            for c in codes:
                error_counts[c] = error_counts.get(c, 0) + 1
            quarantine_row = record.copy()
            quarantine_row["validation_error_codes"] = ";".join(codes)
            quarantine_row["validation_error_messages"] = ";".join(msgs)
            quarantine_row["quarantine_timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            invalid_records.append(quarantine_row)

    metrics = {
        "total_records": len(records),
        "valid_records": len(valid_records),
        "invalid_records": len(invalid_records),
        "error_counts": error_counts
    }
    
    logger.info(
        f"Validation complete: Total={metrics['total_records']}, "
        f"Valid={metrics['valid_records']}, Invalid={metrics['invalid_records']}"
    )
    return valid_records, invalid_records, metrics
