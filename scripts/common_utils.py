"""
common_utils.py
Common utilities, logging, currency exchange rates, and path helpers
for AWS Glue PySpark Incremental Data Pipeline.
"""

import sys
import os
import json
import logging
import hashlib
from datetime import datetime
from typing import Dict, Any, Tuple, Optional

# Static currency conversion reference rates against USD
# In enterprise production, this can be queried from DynamoDB or an API
EXCHANGE_RATES_TO_USD: Dict[str, float] = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "CAD": 0.74,
    "AUD": 0.65,
    "JPY": 0.0067
}

VALID_STATUSES = {"CONFIRMED", "CANCELLED", "PENDING", "COMPLETED"}
VALID_PAYMENT_METHODS = {"CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER", "CRYPTO"}


def get_logger(name: str = "incremental_glue_pipeline", level: int = logging.INFO) -> logging.Logger:
    """Configures and returns a structured logger suitable for AWS CloudWatch and local dev."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)
    return logger


def parse_job_arguments(default_args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parses job arguments seamlessly whether executed in AWS Glue (via getResolvedOptions)
    or in local development / pytest (via argparse or sys.argv).
    """
    args = default_args.copy()
    
    # Try AWS Glue environment first
    try:
        from awsglue.utils import getResolvedOptions
        arg_keys = list(default_args.keys())
        resolved = getResolvedOptions(sys.argv, arg_keys)
        args.update(resolved)
        return args
    except (ImportError, Exception):
        pass

    # Fallback to standard CLI argument parsing
    for key in default_args:
        flag = f"--{key}"
        if flag in sys.argv:
            idx = sys.argv.index(flag)
            if idx + 1 < len(sys.argv):
                args[key] = sys.argv[idx + 1]
                
    return args


def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    """
    Extracts bucket name and prefix from s3://bucket/prefix/path.
    Returns (bucket, prefix).
    """
    clean_uri = s3_uri.replace("s3://", "").replace("s3a://", "")
    parts = clean_uri.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


def compute_record_hash(*values: Any) -> str:
    """Computes a deterministic SHA-256 fingerprint for idempotency and change tracking."""
    normalized = "|".join(str(v).strip() if v is not None else "" for v in values)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def get_partition_path(dt: datetime) -> str:
    """Generates standard Hive partition path segment: year=YYYY/month=MM/day=DD."""
    return f"year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}"


def normalize_currency_amount(amount: float, currency: str) -> Optional[float]:
    """Converts a local currency amount to USD using standardized exchange rates."""
    if amount is None or currency not in EXCHANGE_RATES_TO_USD:
        return None
    rate = EXCHANGE_RATES_TO_USD[currency]
    return round(float(amount) * rate, 2)
