# Architecture & Technical Design: Incremental Travel Data Pipeline

This document details the production architecture, components, data flow, failure recovery model, and design decisions for the serverless **Incremental Data Pipeline with Orchestration** built on AWS.

---

## 1. High-Level Architecture Diagram

```mermaid
flowchart TD
    subgraph Ingestion ["1. Data Ingestion & Landing"]
        A["Daily Travel Booking CSVs<br/>(Day 1, Day 2, Corrupt)"] -->|Upload| B[("Amazon S3 Raw Zone<br/>s3://bucket/raw-data/")]
    end

    subgraph EventDriven ["2. Event-Driven Automation"]
        B -->|S3 Object Created Notification| C["Amazon EventBridge<br/>Rule: prefix='raw-data/' & suffix='.csv'"]
        C -->|Trigger Action| D["AWS Glue Workflow<br/>(travel_bookings_pipeline_workflow)"]
    end

    subgraph Orchestration ["3. Glue Workflow Orchestration"]
        D -->|Step 1: Start| E["AWS Glue Crawler<br/>(travel_bookings_raw_crawler)"]
        E -->|Discovers Schema & Updates| F[("AWS Glue Data Catalog<br/>Database: travel_bookings_db")]
        E -->|On Succeeded Trigger| G["AWS Glue PySpark Job<br/>(incremental_travel_bookings_etl)"]
    end

    subgraph IncrementalETL ["4. PySpark Incremental ETL Processing"]
        G -->|DynamicFrame Read| H{"Glue Job Bookmark<br/>(transformation_ctx)"}
        H -->|New Files Detected| I["PySpark Schema Casting & Trimming"]
        H -->|Already Processed Files| J["Zero Reprocessing<br/>(No-Op & Fast Exit)"]
        
        I --> K{"Data Quality Validation<br/>(data_validation.py)"}
        
        K -->|Defective Records<br/>(Null IDs, Negative Amount,<br/>Invalid Dates, Duplicates)| L[("Amazon S3 Quarantine<br/>s3://bucket/invalid_records/<br/>year=YYYY/month=MM/day=DD/")]
        
        K -->|Valid Records| M["Analytical Feature Engineering<br/>(amount_usd, lead_time_days,<br/>record_hash, partition keys)"]
        
        M -->|Dynamic Partition Overwrite| N[("Amazon S3 Processed Zone<br/>s3://bucket/processed/bookings/<br/>year=YYYY/month=MM/day=DD/<br/>Snappy Parquet")]
        
        N -->|Commit Bookmark State| O["job.commit()<br/>(Saves S3 Object Watermark)"]
    end

    subgraph Analytics ["5. Serverless SQL Analytics"]
        N --> P[("Amazon Athena<br/>travel_bookings_db.processed_bookings")]
        L --> Q[("Amazon Athena<br/>travel_bookings_db.quarantined_bookings")]
        P --> R["Business Analytics Queries<br/>(Revenue, Trends, Destinations)"]
        Q --> S["Data Quality Audit Queries<br/>(Error Code Aggregation)"]
    end
```

---

## 2. Core Architectural Principles & Components

### 2.1 Amazon S3 Data Lake Hierarchy
The S3 bucket is organized into isolated functional zones to adhere to data lake separation of concerns:

| S3 Prefix | Format | Lifecycle / Retention | Description |
|---|---|---|---|
| `raw-data/` | CSV (UTF-8) | Expire after 90 days | Immutable landing zone for incoming batch files. |
| `processed/bookings/` | Snappy Parquet | Standard / Intelligent Tiering | Cleaned, enriched, partitioned analytics layer. |
| `invalid_records/` | Snappy Parquet | Expire after 180 days | Quarantined records with error code tags. |
| `scripts/` | Python (`.py`) | Retain indefinitely | PySpark job code and helper utility modules. |
| `spark-logs/` | JSON / EventLog | Expire after 30 days | Spark UI event logs for profiling and tuning. |
| `athena-results/` | CSV / Metadata | Expire after 7 days | Temporary Athena query output location. |

### 2.2 Event-Driven Detection via Amazon EventBridge
Rather than polling S3 on a recurring cron (which wastes compute and delays processing), the architecture leverages Amazon S3 Event Notifications published directly to **Amazon EventBridge**:
- **Bucket-Level Setting**: `EventBridgeConfiguration: { EventBridgeEnabled: true }`.
- **Event Filter Rule**:
  ```json
  {
    "source": ["aws.s3"],
    "detail-type": ["Object Created"],
    "detail": {
      "bucket": { "name": ["<data-lake-bucket>"] },
      "object": {
        "key": [{ "prefix": "raw-data/" }, { "suffix": ".csv" }]
      }
    }
  }
  ```
- **Noise Prevention**: Files uploaded to `scripts/`, `processed/`, or temporary staging paths do not trigger executions.

### 2.3 Schema Discovery via AWS Glue Crawler
The AWS Glue Crawler (`travel_bookings_raw_crawler`) runs at the beginning of the workflow:
- Scans `s3://<bucket>/raw-data/` to discover newly added columns or schema alterations.
- Updates table metadata in the AWS Glue Data Catalog (`travel_bookings_db.raw_bookings_csv`).
- Configured with `CRAWL_NEW_FOLDERS_ONLY` to eliminate unnecessary S3 list requests.

### 2.4 Incremental Processing with AWS Glue Job Bookmarks
AWS Glue Job Bookmarks maintain state between job runs by tracking source object keys, last modified timestamps, ETags, and version IDs.
- **DynamicFrame Source Context**:
  ```python
  raw_dyf = glueContext.create_dynamic_frame.from_options(
      connection_type="s3",
      connection_options={"paths": [raw_s3_path], "recurse": True},
      format="csv",
      format_options={"withHeader": True, "separator": ","},
      transformation_ctx="raw_bookings_csv_source"
  )
  ```
- **Execution Flow**:
  - **Batch 1 (Initial)**: Ingests all historical files; stores high watermark.
  - **Batch 2 (Incremental)**: Reads only newly uploaded CSVs; avoids re-reading Batch 1.
  - **Batch 3 (No new files)**: DynamicFrame returns 0 rows; job logs zero records and exits without reprocessing.

---

## 3. Idempotency & Job Failure Strategy

### The Limitations of Job Bookmarks
1. **File-Level Watermarking**: Glue bookmarks record *file metadata*, not row-level primary keys.
2. **Failure Gap**: If a job crashes midway after writing 4 out of 10 partitions, `job.commit()` does NOT run. On the subsequent retry, the bookmark re-reads the exact same input files.
3. **Append-Mode Duplication**: If standard `.mode("append")` is used, the retry writes duplicate parquet files into the already-written partitions!

### Our Multi-Tiered Idempotency Solution
To achieve true end-to-end idempotency:

1. **Dynamic Partition Overwrite**:
   ```python
   spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
   valid_df.repartition("year", "month", "day") \
       .write \
       .mode("overwrite") \
       .partitionBy("year", "month", "day") \
       .parquet(processed_s3_path)
   ```
   *Behavior*: If a job re-runs, Spark dynamically replaces *only* the specific `year/month/day` partitions present in the current batch. Unaffected historical partitions remain intact.
2. **Deterministic Record Hashing**:
   Each record is assigned a SHA-256 fingerprint:
   $$\text{record\_hash} = \text{SHA256}(\text{booking\_id} \parallel \text{customer\_id} \parallel \text{amount\_usd} \parallel \text{booking\_status})$$
3. **Late-Arrival & Batch Deduplication**:
   Within the batch, `dropDuplicates(["booking_id"])` eliminates duplicate records before writing.

---

## 4. Data Quality & Dead-Letter Routing

Instead of silently dropping defective records (which causes blind data loss and reconcilation nightmares), records are partitioned into two streams:

```
Raw Batch Records
       │
       ▼
 [Data Quality Engine]
       ├── Valid Records ───────────► Processed Parquet (year/month/day)
       └── Defective Records ───────► Dead-Letter Quarantine (year/month/day)
                                      + validation_error_codes
                                      + quarantine_timestamp
```

### Validation Rules Enforced:
1. `booking_id`: Non-empty and matches format `BK-YYYYMMDD-XXXX`.
2. `customer_id`: Non-empty and matches format `CUST-XXXX`.
3. `amount`: Numeric and strictly $> 0.00$.
4. `currency`: Supported ISO code (`USD`, `EUR`, `GBP`, `CAD`, `AUD`, `JPY`).
5. `booking_status`: Valid status (`CONFIRMED`, `CANCELLED`, `PENDING`, `COMPLETED`).
6. `dates`: ISO formatted and $\text{travel\_date} \ge \text{booking\_date}$.
7. `deduplication`: In-batch unique `booking_id`.
