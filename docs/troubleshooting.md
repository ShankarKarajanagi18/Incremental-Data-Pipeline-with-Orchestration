# Troubleshooting & Operational Runbook: AWS Glue Incremental Data Pipeline

This runbook covers root causes, debugging steps, and remediation playbooks for common operational challenges encountered in production AWS Glue and EventBridge incremental data pipelines.

---

## 1. AWS Glue Job Bookmarks Issues

### Problem 1.1: Job Bookmark reprocesses already-processed files
- **Symptom**: The Glue Job reads all historical CSV files instead of only new files.
- **Root Causes**:
  1. `--job-bookmark-option` was omitted or set to `job-bookmark-disable`.
  2. The `transformation_ctx` argument was omitted or changed in `glueContext.create_dynamic_frame.from_options()`.
  3. Source S3 files were overwritten with new timestamps.
- **Remediation**:
  - Verify job arguments include `'--job-bookmark-option': 'job-bookmark-enable'`.
  - Ensure `transformation_ctx` has a consistent, unique string identifier (e.g. `raw_bookings_csv_source`).
  - If a clean historical replay is intentionally required, reset the bookmark:
    ```bash
    aws glue reset-job-bookmark --job-name incremental_travel_bookings_etl
    ```

### Problem 1.2: Job Bookmark skips newly uploaded files
- **Symptom**: New files uploaded to S3 are ignored by the DynamicFrame.
- **Root Cause**: The files were uploaded with timestamps older than the last committed bookmark watermark (e.g., historical backfill files with preserved original timestamps).
- **Remediation**:
  - Touch or re-upload files with current timestamps, or run a dedicated historical backfill job with bookmark paused (`job-bookmark-pause`).

---

## 2. Duplicate Records & Idempotency Issues

### Problem 2.1: Duplicate records in S3 processed partitions after job failure/retry
- **Symptom**: Athena queries return duplicated booking IDs after a job failure was recovered.
- **Root Cause**:
  - When a Glue job fails before `job.commit()`, the bookmark state rolls back to the previous successful run.
  - If the write operation used standard Spark `.mode("append")`, the rerun appends another copy of the records into S3.
- **Remediation**:
  - Enforce dynamic partition overwrite in the Glue Job configuration:
    ```python
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(...)
    ```
  - In Athena, run deduplicated queries using analytical window functions:
    ```sql
    WITH ranked_bookings AS (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY booking_id ORDER BY ingestion_timestamp DESC) as rn
        FROM travel_bookings_db.processed_bookings
    )
    SELECT * FROM ranked_bookings WHERE rn = 1;
    ```

---

## 3. EventBridge & Workflow Automation Issues

### Problem 3.1: Glue Workflow does not trigger when CSV is uploaded to S3
- **Symptom**: File is uploaded to `s3://bucket/raw-data/bookings.csv`, but no Workflow run starts.
- **Diagnosis Checklist**:
  1. Check if S3 EventBridge notifications are enabled on the bucket:
     ```bash
     aws s3api get-bucket-notification-configuration --bucket <BUCKET_NAME>
     ```
     Ensure `EventBridgeConfiguration` is present.
  2. Check EventBridge Rule state:
     ```bash
     aws events describe-rule --name travel_bookings_s3_upload_rule
     ```
     State must be `ENABLED`.
  3. Check IAM Role permissions:
     The `EventBridgeGlueRole` must have permission for `glue:notifyEvent` and `glue:StartWorkflowRun`.
  4. Check file prefix and suffix:
     The rule strictly filters on prefix `raw-data/` and suffix `.csv`. Uploading to root or without `.csv` extension will not trigger.

---

## 4. Amazon Athena Query Issues

### Problem 4.1: Athena returns 0 rows or missing recent partitions
- **Symptom**: `SELECT COUNT(*) FROM travel_bookings_db.processed_bookings` returns 0 even though files exist in S3.
- **Root Cause**: Hive metastore partition metadata is not yet updated for the new `year/month/day` subdirectories.
- **Remediation**:
  - Run MSCK repair in Athena:
    ```sql
    MSCK REPAIR TABLE travel_bookings_db.processed_bookings;
    ```
  - Alternatively, configure the Glue Crawler to recrawl the processed zone at the end of the workflow.

---

## 5. Glue Job Execution & Python Dependency Errors

### Problem 5.1: `ModuleNotFoundError: No module named 'common_utils'`
- **Symptom**: Glue Job crashes immediately during initialization.
- **Root Cause**: The helper modules `common_utils.py` and `data_validation.py` were not packaged or the S3 URI passed in `--extra-py-files` is incorrect.
- **Remediation**:
  - Verify files exist in S3:
    ```bash
    aws s3 ls s3://<BUCKET_NAME>/scripts/
    ```
  - Verify Glue Job parameter:
    `--extra-py-files: s3://<BUCKET_NAME>/scripts/common_utils.py,s3://<BUCKET_NAME>/scripts/data_validation.py`
