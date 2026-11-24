# AWS Deployment Guide: Incremental Travel Data Pipeline

This guide provides step-by-step instructions for deploying the production pipeline on AWS using AWS CloudFormation and the AWS CLI.

---

## 1. Prerequisites

Before starting, ensure you have:
1. **AWS Account**: With permissions for CloudFormation, S3, IAM, AWS Glue, EventBridge, and Athena.
2. **AWS CLI v2**: Installed and authenticated (`aws configure` or SSO).
3. **Python 3.10+**: For running local tests and synthetic data generators.

Verify AWS CLI configuration:
```bash
aws sts get-caller-identity
```

---

## 2. Step-by-Step Deployment

### Step 1: Deploy CloudFormation Infrastructure
Deploy the CloudFormation template to provision the S3 data lake bucket, IAM roles, Glue Catalog database, Glue Crawler, Glue ETL Job, Glue Workflow, and EventBridge Rule:

```bash
aws cloudformation deploy \
  --template-file infrastructure/cloudformation.yaml \
  --stack-name travel-booking-pipeline \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides Environment=dev GlueWorkerType=G.1X NumberOfWorkers=2
```

Retrieve the provisioned S3 Bucket Name from stack outputs:
```bash
BUCKET_NAME=$(aws cloudformation describe-stacks \
  --stack-name travel-booking-pipeline \
  --query "Stacks[0].Outputs[?OutputKey=='DataLakeBucketName'].OutputValue" \
  --output text)

echo "Provisioned S3 Bucket: ${BUCKET_NAME}"
```

---

### Step 2: Upload PySpark Scripts and Dependent Modules to S3
The AWS Glue ETL Job requires the main script and the helper modules (`common_utils.py` and `data_validation.py` passed via `--extra-py-files`):

```bash
# Upload ETL script
aws s3 cp scripts/glue_etl_job.py s3://${BUCKET_NAME}/scripts/glue_etl_job.py

# Upload dependent python packages
aws s3 cp scripts/common_utils.py s3://${BUCKET_NAME}/scripts/common_utils.py
aws s3 cp scripts/data_validation.py s3://${BUCKET_NAME}/scripts/data_validation.py
```

---

### Step 3: Test 1 - Upload Initial Batch (Day 1)
Generate Day 1 clean dataset (100 records) and upload to the raw-data landing prefix:

```bash
# Generate test batches locally
python scripts/generate_data.py --generate-all-test-batches --output-dir data/sample_bookings

# Upload Day 1 CSV
aws s3 cp data/sample_bookings/bookings_2026-11-25.csv s3://${BUCKET_NAME}/raw-data/bookings_2026-11-25.csv
```

---

### Step 4: Verify EventBridge and Glue Workflow Execution
Once the CSV arrives in `raw-data/`, EventBridge triggers the Glue Workflow:

1. Check Workflow Run status:
   ```bash
   aws glue get-workflow-runs \
     --name travel_bookings_pipeline_workflow \
     --include-graph
   ```
2. The workflow will:
   - Run `travel_bookings_raw_crawler` to discover schema in `raw-data/`.
   - On crawler success, automatically trigger `incremental_travel_bookings_etl`.
3. Check CloudWatch logs for the Glue Job:
   ```bash
   aws logs tail /aws-glue/jobs/output --follow
   ```

Verify processed Parquet output in S3:
```bash
aws s3 ls s3://${BUCKET_NAME}/processed/bookings/ --recursive
```
*Expected*: Partition folder `year=2026/month=11/day=25/` containing Snappy-compressed `.parquet` files with 100 rows.

---

### Step 5: Test 2 - Incremental Processing (Day 2 Upload)
Upload the Day 2 batch (50 new records) to verify that AWS Glue Job Bookmarks process **only** new files:

```bash
aws s3 cp data/sample_bookings/bookings_2026-11-26.csv s3://${BUCKET_NAME}/raw-data/bookings_2026-11-26.csv
```

Monitor the Glue Job execution logs.
*Expected Result*:
- Job Bookmark discovers only 1 new file (`bookings_2026-11-26.csv`).
- Ingests exactly 50 records.
- Creates new partition `year=2026/month=11/day=26/`.
- Day 1 partition (`day=25`) is untouched.

---

### Step 6: Test 3 - Zero Reprocessing (No-Op Run)
Run the Glue Job manually without uploading any new files:

```bash
aws glue start-job-run --job-name incremental_travel_bookings_etl
```

Check logs:
*Expected Result*:
```json
{"message": "Discovered 0 new records via Glue Job Bookmark."}
{"message": "No new files detected by Job Bookmark. Committing job and exiting gracefully."}
```
No redundant data is processed or rewritten.

---

### Step 7: Test 4 - Data Quality Checks & Quarantine Dead-Letter Routing
Upload the corrupt batch (Day 3) containing missing IDs, negative amounts, invalid dates, and duplicate records:

```bash
aws s3 cp data/sample_bookings/bookings_2026-11-27_corrupt.csv s3://${BUCKET_NAME}/raw-data/bookings_2026-11-27_corrupt.csv
```

Inspect S3 quarantine directory:
```bash
aws s3 ls s3://${BUCKET_NAME}/invalid_records/ --recursive
```
*Expected Result*:
- Clean records written to `processed/bookings/year=2026/month=11/day=27/`.
- Corrupt records written to `invalid_records/year=2026/month=11/day=27/` with attached `validation_error_codes` and `quarantine_timestamp`.

---

### Step 8: Configure and Query with Amazon Athena
1. Open the Amazon Athena Query Editor in the AWS Console (or use AWS CLI).
2. Set the Query Result location:
   ```sql
   s3://<BUCKET_NAME>/athena-results/
   ```
3. Run the DDL scripts in `sql/create_tables.sql`:
   - Creates database `travel_bookings_db`.
   - Creates external table `processed_bookings`.
   - Creates external table `quarantined_bookings`.
4. Load partitions:
   ```sql
   MSCK REPAIR TABLE travel_bookings_db.processed_bookings;
   MSCK REPAIR TABLE travel_bookings_db.quarantined_bookings;
   ```
5. Execute analytics queries from `sql/revenue_analysis.sql` and `sql/booking_analysis.sql`.

---

## 3. Cost Control & Resource Cleanup

### Cost Breakdown & Minimization Tactics
| Service | Charge Model | Mitigation Strategy in Project |
|---|---|---|
| **AWS Glue Job** | $0.44 per DPU-Hour | Set `WorkerType: G.1X`, `NumberOfWorkers: 2`, `Timeout: 15 min`. Short runtime (~2-3 mins). |
| **AWS Glue Crawler** | $0.44 per DPU-Hour | Set `RecrawlBehavior: CRAWL_NEW_FOLDERS_ONLY`. |
| **Amazon S3** | $0.023 per GB/mo | Small synthetic datasets (< 5 MB); Lifecycle rule expires raw data after 90 days. |
| **Amazon EventBridge** | Free tier (1M events/mo) | Free for S3 bucket notifications. |
| **Amazon Athena** | $5.00 per TB scanned | Partition pruning on `year/month/day` and columnar Parquet + Snappy cuts scan sizes to Kilobytes. |

### Resource Cleanup (Tear Down)
To completely delete all AWS resources and ensure zero recurring charges:

```bash
# 1. Empty the S3 Data Lake Bucket (required before bucket deletion)
aws s3 rm s3://${BUCKET_NAME} --recursive

# 2. Delete the CloudFormation Stack
aws cloudformation delete-stack --stack-name travel-booking-pipeline

# 3. Wait for deletion to complete
aws cloudformation wait stack-delete-complete --stack-name travel-booking-pipeline

echo "All AWS resources successfully torn down."
```
