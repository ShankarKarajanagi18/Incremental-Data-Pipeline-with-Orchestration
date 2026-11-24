"""
glue_etl_job.py
Production AWS Glue PySpark ETL Job for Incremental Travel Booking Pipeline.

Key Architecture & Engineering Principles:
1. Incremental Ingestion:
   - Uses AWS Glue Job Bookmarks via DynamicFrame transformation_ctx.
   - Reads only new or modified CSV files from S3 raw-data prefix.
2. Data Quality & Quarantining:
   - Validates data against strict schema, positive amounts, chronological dates.
   - Routes invalid / corrupted rows to dead-letter S3 prefix instead of silent dropping.
3. Analytical Enrichment:
   - Calculates lead_time_days (travel_date - booking_date).
   - Normalizes revenue to USD (amount_usd) using exchange rate map.
   - Generates deterministic record_hash for idempotency and auditability.
   - Adds Hive-compatible partition keys: year, month, day.
4. Idempotent Storage:
   - Output written in Snappy-compressed Parquet.
   - Dynamic partition overwrite mode prevents duplicate files on job retries.
   - Coalesces partitions to avoid small-file proliferation.
5. Glue Job Lifecycle:
   - Initializes job with Job Bookmarks enabled.
   - Commits bookmark state only after all write operations succeed.
"""

import sys
import os

# Ensure parent directory and current directory are on sys.path for local runs
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from datetime import datetime
from typing import Dict, Any

# Dynamic imports to support both AWS Glue cluster and local test execution
try:
    from pyspark.context import SparkContext
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, 
        IntegerType, TimestampType, DateType
    )
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.utils import getResolvedOptions
    GLUE_RUNTIME_AVAILABLE = True
except ImportError:
    GLUE_RUNTIME_AVAILABLE = False
    SparkSession = None
    F = None

# Local module imports
try:
    from scripts.common_utils import (
        get_logger, parse_job_arguments, EXCHANGE_RATES_TO_USD
    )
    from scripts.data_validation import ValidationErrorCodes
except ImportError:
    from common_utils import (
        get_logger, parse_job_arguments, EXCHANGE_RATES_TO_USD
    )
    from data_validation import ValidationErrorCodes

logger = get_logger("glue_etl_job")

# Raw CSV schema definition
RAW_SCHEMA = [
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


def build_exchange_rate_expr():
    """Builds a PySpark when-otherwise expression for currency conversion to USD."""
    expr = F.when(F.col("currency") == "USD", F.lit(1.0))
    for curr, rate in EXCHANGE_RATES_TO_USD.items():
        if curr != "USD":
            expr = expr.when(F.col("currency") == curr, F.lit(rate))
    return expr.otherwise(F.lit(None))


def transform_and_enrich_dataframe(df):
    """
    Applies standardization, type casting, validation flags, 
    and analytical feature engineering to the Spark DataFrame.
    """
    # 1. Clean and normalize column names (lowercase & trimmed)
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.strip().lower())

    # 2. String trimming
    string_cols = ["booking_id", "customer_id", "destination", "currency", "booking_status", "payment_method"]
    for sc in string_cols:
        if sc in df.columns:
            df = df.withColumn(sc, F.trim(F.col(sc)))

    # 3. Type casting & Parsing
    df = df.withColumn("amount_numeric", F.col("amount").cast(DoubleType())) \
           .withColumn("booking_timestamp", F.to_timestamp(F.col("booking_date"), "yyyy-MM-dd HH:mm:ss")) \
           .withColumn("travel_date_parsed", F.to_date(F.col("travel_date"), "yyyy-MM-dd"))

    # Fallback parsing for alternative date formats if standard parse produced null
    df = df.withColumn(
        "booking_timestamp",
        F.coalesce(
            F.col("booking_timestamp"),
            F.to_timestamp(F.col("booking_date"), "yyyy-MM-dd"),
            F.to_timestamp(F.col("booking_date"), "yyyy-MM-dd'T'HH:mm:ss")
        )
    )

    # 4. Data Quality Validation Conditions
    cond_valid_booking_id = (
        F.col("booking_id").isNotNull() & 
        (F.col("booking_id") != "") & 
        F.col("booking_id").rlike(r"^BK-\d{8}-\d{3,6}$")
    )
    cond_valid_customer_id = (
        F.col("customer_id").isNotNull() & 
        (F.col("customer_id") != "") & 
        F.col("customer_id").rlike(r"^CUST-\d{3,6}$")
    )
    cond_valid_amount = (
        F.col("amount_numeric").isNotNull() & 
        (F.col("amount_numeric") > 0.0)
    )
    cond_valid_dates = (
        F.col("booking_timestamp").isNotNull() & 
        F.col("travel_date_parsed").isNotNull() & 
        (F.col("travel_date_parsed") >= F.to_date(F.col("booking_timestamp")))
    )
    valid_currencies = list(EXCHANGE_RATES_TO_USD.keys())
    cond_valid_currency = F.col("currency").isin(valid_currencies)
    valid_statuses = ["CONFIRMED", "CANCELLED", "PENDING", "COMPLETED"]
    cond_valid_status = F.col("booking_status").isin(valid_statuses)

    # Overall validity flag
    df = df.withColumn(
        "is_valid_record",
        cond_valid_booking_id & cond_valid_customer_id & cond_valid_amount & 
        cond_valid_dates & cond_valid_currency & cond_valid_status
    )

    # Record error tags for invalid rows
    error_tag_expr = F.array_compact(F.array(
        F.when(~cond_valid_booking_id, F.lit(ValidationErrorCodes.MALFORMED_BOOKING_ID)),
        F.when(~cond_valid_customer_id, F.lit(ValidationErrorCodes.MALFORMED_CUSTOMER_ID)),
        F.when(~cond_valid_amount, F.lit(ValidationErrorCodes.INVALID_AMOUNT)),
        F.when(~cond_valid_dates, F.lit(ValidationErrorCodes.CHRONOLOGY_VIOLATION)),
        F.when(~cond_valid_currency, F.lit(ValidationErrorCodes.INVALID_CURRENCY)),
        F.when(~cond_valid_status, F.lit(ValidationErrorCodes.INVALID_STATUS))
    ))
    df = df.withColumn("validation_error_codes", F.concat_ws(";", error_tag_expr))

    # 5. Feature Engineering for Valid Records
    rate_multiplier = build_exchange_rate_expr()
    df = df.withColumn("exchange_rate_usd", rate_multiplier) \
           .withColumn("amount_usd", F.round(F.col("amount_numeric") * F.col("exchange_rate_usd"), 2)) \
           .withColumn("lead_time_days", F.datediff(F.col("travel_date_parsed"), F.to_date(F.col("booking_timestamp")))) \
           .withColumn("year", F.year(F.col("booking_timestamp")).cast(StringType())) \
           .withColumn("month", F.lpad(F.month(F.col("booking_timestamp")).cast(StringType()), 2, "0")) \
           .withColumn("day", F.lpad(F.dayofmonth(F.col("booking_timestamp")).cast(StringType()), 2, "0")) \
           .withColumn("ingestion_timestamp", F.current_timestamp())

    # Deterministic record hash for idempotency and change tracking
    df = df.withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws("||", 
                F.coalesce(F.col("booking_id"), F.lit("")),
                F.coalesce(F.col("customer_id"), F.lit("")),
                F.coalesce(F.col("amount_usd").cast(StringType()), F.lit("")),
                F.coalesce(F.col("booking_status"), F.lit(""))
            ),
            256
        )
    )

    return df


def split_valid_and_invalid(df):
    """
    Separates the processed DataFrame into valid and quarantined DataFrames.
    Applies deduplication on booking_id for valid records.
    """
    # Valid records: filter flag, deduplicate on booking_id, drop temp columns
    valid_df = df.filter(F.col("is_valid_record") == True) \
                 .dropDuplicates(["booking_id"]) \
                 .select(
                     "booking_id",
                     "customer_id",
                     "destination",
                     F.col("booking_timestamp").alias("booking_date"),
                     F.col("travel_date_parsed").alias("travel_date"),
                     F.col("amount_numeric").alias("amount"),
                     "currency",
                     "amount_usd",
                     "booking_status",
                     "payment_method",
                     "lead_time_days",
                     "record_hash",
                     "ingestion_timestamp",
                     "year",
                     "month",
                     "day"
                 )

    # Invalid records: filter invalid, preserve raw fields and attach errors
    invalid_df = df.filter(F.col("is_valid_record") == False) \
                   .select(
                       "booking_id",
                       "customer_id",
                       "destination",
                       "booking_date",
                       "travel_date",
                       "amount",
                       "currency",
                       "booking_status",
                       "payment_method",
                       "validation_error_codes",
                       F.current_timestamp().alias("quarantine_timestamp"),
                       F.coalesce(F.col("year"), F.date_format(F.current_timestamp(), "yyyy")).alias("year"),
                       F.coalesce(F.col("month"), F.date_format(F.current_timestamp(), "MM")).alias("month"),
                       F.coalesce(F.col("day"), F.date_format(F.current_timestamp(), "dd")).alias("day")
                   )

    return valid_df, invalid_df


def run_pipeline(job_args: Dict[str, Any]):
    """
    Main pipeline execution logic.
    Configured to run both in AWS Glue environment and in local PySpark environment.
    """
    logger.info("Initializing Incremental Travel Data Pipeline ETL Job...")
    
    raw_s3_path = job_args.get("RAW_S3_PATH", "s3://travel-booking-pipeline-data/raw-data/")
    processed_s3_path = job_args.get("PROCESSED_S3_PATH", "s3://travel-booking-pipeline-data/processed/bookings/")
    invalid_s3_path = job_args.get("INVALID_S3_PATH", "s3://travel-booking-pipeline-data/invalid_records/")
    job_name = job_args.get("JOB_NAME", "incremental_travel_bookings_etl")
    
    logger.info(f"Configuration: RAW_PATH={raw_s3_path}, PROCESSED_PATH={processed_s3_path}, INVALID_PATH={invalid_s3_path}")

    # Set up Spark and Glue Contexts
    if GLUE_RUNTIME_AVAILABLE:
        sc = SparkContext.getOrCreate()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(job_name, job_args)
    else:
        logger.warning("AWS Glue runtime not detected. Initializing standard SparkSession...")
        spark = SparkSession.builder \
            .appName(job_name) \
            .master("local[*]") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()
        glueContext = None
        job = None

    # Enable dynamic partition overwrite to achieve idempotency on job retries
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # Step 1: Read raw CSV using Job Bookmarks
    if glueContext:
        logger.info("Reading raw CSV via Glue DynamicFrame with Job Bookmark enabled...")
        raw_dyf = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [raw_s3_path], "recurse": True},
            format="csv",
            format_options={"withHeader": True, "separator": ","},
            transformation_ctx="raw_bookings_csv_source"
        )
        record_count = raw_dyf.count()
        logger.info(f"Discovered {record_count} new records via Glue Job Bookmark.")
        
        if record_count == 0:
            logger.info("No new files detected by Job Bookmark. Committing job and exiting gracefully.")
            job.commit()
            return
            
        raw_df = raw_dyf.toDF()
    else:
        logger.info(f"Reading CSV from path: {raw_s3_path}")
        raw_df = spark.read.option("header", "true").csv(raw_s3_path)

    # Step 2: Transform and Validate
    logger.info("Applying transformations, business validations, and feature engineering...")
    enriched_df = transform_and_enrich_dataframe(raw_df)
    valid_df, invalid_df = split_valid_and_invalid(enriched_df)

    # Step 3: Write Valid Records to Processed Parquet (Idempotent Partitioning)
    valid_count = valid_df.count()
    logger.info(f"Writing {valid_count} valid records to processed zone: {processed_s3_path}")
    if valid_count > 0:
        # Coalesce to avoid small files while maintaining partition distribution
        valid_df.repartition("year", "month", "day") \
            .write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .option("compression", "snappy") \
            .parquet(processed_s3_path)
        logger.info("Successfully wrote valid records to processed Parquet.")

    # Step 4: Write Invalid Records to Quarantine Dead-Letter S3 Prefix
    invalid_count = invalid_df.count()
    if invalid_count > 0:
        logger.warning(f"Quarantining {invalid_count} invalid records to {invalid_s3_path}")
        invalid_df.repartition("year", "month", "day") \
            .write \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .option("compression", "snappy") \
            .parquet(invalid_s3_path)
        logger.info("Successfully stored quarantined invalid records.")

    # Step 5: Commit Glue Job Bookmark
    if job:
        logger.info("Committing AWS Glue Job Bookmark state...")
        job.commit()
        logger.info("Glue Job Bookmark committed successfully.")


if __name__ == "__main__":
    default_parameters = {
        "JOB_NAME": "incremental_travel_bookings_etl",
        "RAW_S3_PATH": "s3://travel-booking-pipeline-data/raw-data/",
        "PROCESSED_S3_PATH": "s3://travel-booking-pipeline-data/processed/bookings/",
        "INVALID_S3_PATH": "s3://travel-booking-pipeline-data/invalid_records/"
    }
    resolved_args = parse_job_arguments(default_parameters)
    run_pipeline(resolved_args)
