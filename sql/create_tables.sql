-- ============================================================================
-- create_tables.sql
-- Amazon Athena / AWS Glue Data Catalog DDL definitions for Travel Bookings Data Lake
-- ============================================================================

-- 1. Create Analytics Database
CREATE DATABASE IF NOT EXISTS travel_bookings_db
LOCATION 's3://travel-booking-pipeline-data/database/'
COMMENT 'Analytical database for incremental travel booking pipeline';

-- ----------------------------------------------------------------------------
-- 2. Processed Parquet Table (Optimized Analytics Layer)
-- ----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS travel_bookings_db.processed_bookings (
    booking_id STRING COMMENT 'Unique booking reference (e.g. BK-20261125-1001)',
    customer_id STRING COMMENT 'Unique customer reference (e.g. CUST-4819)',
    destination STRING COMMENT 'Destination city / airport',
    booking_date TIMESTAMP COMMENT 'Timestamp when booking was created',
    travel_date DATE COMMENT 'Date of scheduled travel',
    amount DOUBLE COMMENT 'Transaction booking amount in local currency',
    currency STRING COMMENT 'ISO 4217 Currency Code (USD, EUR, GBP, CAD, AUD, JPY)',
    amount_usd DOUBLE COMMENT 'Standardized revenue normalized to USD',
    booking_status STRING COMMENT 'Booking lifecycle status (CONFIRMED, CANCELLED, PENDING, COMPLETED)',
    payment_method STRING COMMENT 'Payment channel (CREDIT_CARD, DEBIT_CARD, PAYPAL, BANK_TRANSFER, CRYPTO)',
    lead_time_days INT COMMENT 'Days between booking date and scheduled travel date',
    record_hash STRING COMMENT 'SHA-256 fingerprint for idempotency & CDC audit',
    ingestion_timestamp TIMESTAMP COMMENT 'UTC timestamp when record was processed into Parquet'
)
COMMENT 'Transformed, cleaned, and enriched travel bookings stored in Snappy Parquet'
PARTITIONED BY (
    year STRING COMMENT 'Hive partition year (YYYY)',
    month STRING COMMENT 'Hive partition month (MM)',
    day STRING COMMENT 'Hive partition day (DD)'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://travel-booking-pipeline-data/processed/bookings/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification' = 'parquet'
);

-- Command to load newly created S3 partitions into Athena catalog
MSCK REPAIR TABLE travel_bookings_db.processed_bookings;


-- ----------------------------------------------------------------------------
-- 3. Dead-Letter Quarantine Table (Invalid / Corrupted Records)
-- ----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS travel_bookings_db.quarantined_bookings (
    booking_id STRING,
    customer_id STRING,
    destination STRING,
    booking_date STRING,
    travel_date STRING,
    amount STRING,
    currency STRING,
    booking_status STRING,
    payment_method STRING,
    validation_error_codes STRING COMMENT 'Semicolon-separated validation error flags',
    quarantine_timestamp TIMESTAMP COMMENT 'Timestamp when record was quarantined'
)
COMMENT 'Quarantined invalid records filtered out during Glue data quality validation'
PARTITIONED BY (
    year STRING,
    month STRING,
    day STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://travel-booking-pipeline-data/invalid_records/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification' = 'parquet'
);

MSCK REPAIR TABLE travel_bookings_db.quarantined_bookings;


-- ----------------------------------------------------------------------------
-- 4. Raw CSV Source Table (Discovered by AWS Glue Crawler)
-- ----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS travel_bookings_db.raw_bookings_csv (
    booking_id STRING,
    customer_id STRING,
    destination STRING,
    booking_date STRING,
    travel_date STRING,
    amount STRING,
    currency STRING,
    booking_status STRING,
    payment_method STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://travel-booking-pipeline-data/raw-data/'
TBLPROPERTIES (
    'skip.header.line.count' = '1'
);
