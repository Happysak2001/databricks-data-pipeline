# Databricks Data Pipeline Project

An automated, event-driven data pipeline built using **Databricks**, **Apache Spark**, **AWS S3**, and **AWS Lambda**. Implements the **Medallion Architecture** (Bronze → Silver → Gold) to ingest, clean, aggregate, and report on transactional and log data.

---

## Architecture Overview

```
New file uploaded to S3
        ↓
S3 Event Notification
        ↓
AWS Lambda (trigger)
        ↓
Databricks Job (charitha-db-pipeline)
        ↓
┌─────────────────────────────────────┐
│  01_ingestion   → Bronze Layer      │
│  02_transformation → Silver Layer   │
│  03_aggregation → Gold Layer        │
│  04_reporting   → Business Reports  │
└─────────────────────────────────────┘
```

---

## Medallion Architecture

| Layer | Table | Description |
|-------|-------|-------------|
| Bronze | `bronze_transactions`, `bronze_logs` | Raw data as-is from S3 |
| Silver | `silver_transactions`, `silver_logs` | Cleaned, validated, partitioned by date |
| Gold | `gold_daily_sales_summary`, `gold_category_summary`, `gold_user_summary`, `gold_log_summary`, `gold_error_trends` | Aggregated, business-ready |

---

## Tech Stack

| Technology | Purpose |
|------------|---------|
| Apache Spark / PySpark | Data processing and transformations |
| Databricks | Notebook execution, Delta Lake, Jobs |
| Delta Lake | Storage format (Bronze/Silver/Gold) |
| AWS S3 | Raw data storage |
| AWS Lambda | Event-driven pipeline trigger |
| Python / boto3 | S3 integration, Lambda function |
| SQL | Reporting queries |

---

## Project Structure

```
databricks-data-pipeline/
├── .github/
│   └── workflows/
│       └── pipeline.yml        # GitHub Actions CI/CD workflow
├── data/
│   ├── transactions.csv        # Sample financial transactions (60 rows)
│   ├── logs.json               # Sample system logs (60 rows)
│   └── test_transactions.csv   # Test file for pipeline validation
├── notebooks/
│   ├── 01_ingestion.py         # Bronze: ingest from S3 → Delta tables
│   ├── 02_transformation.py    # Silver: clean, validate, partition
│   ├── 03_aggregation.py       # Gold: summarize for reporting
│   └── 04_reporting.py         # Business reports via SQL queries
├── tests/
│   ├── test_schema.py          # Schema and data quality validation tests
│   └── test_transformation.py  # PySpark transformation logic tests
├── lambda/
│   └── lambda_function.py      # AWS Lambda trigger function
├── utils/
│   └── helpers.py              # Reusable utility functions
├── requirements.txt            # Python dependencies
└── README.md
```

---

## Data Description

### transactions.csv
| Column | Description |
|--------|-------------|
| transaction_id | Unique transaction identifier |
| user_id | Customer identifier |
| amount | Transaction amount in USD |
| category | Purchase category (Electronics, Groceries, etc.) |
| status | completed / failed |
| timestamp | Transaction datetime |
| merchant | Store/platform name |

### logs.json
| Column | Description |
|--------|-------------|
| log_id | Unique log identifier |
| timestamp | Event datetime |
| level | INFO / WARN / ERROR |
| service | payment-service / auth-service / inventory-service |
| message | Log description |
| user_id | Associated user |
| duration_ms | Response time in milliseconds |

---

## Pipeline Notebooks

### 01 — Ingestion (Bronze Layer)
- Accepts `bucket_name` and `file_key` as job parameters from Lambda
- Reads CSV/JSON from S3 using `boto3`
- Detects file type automatically (.csv → transactions, .json → logs)
- Writes raw data to Bronze Delta managed tables

### 02 — Transformation (Silver Layer)
- Casts `amount` to DoubleType, fills nulls
- Standardizes `category` and `status` to UPPERCASE
- Parses timestamp strings to TimestampType
- Flags slow log events (`duration_ms > 1000ms`)
- Partitions output tables by date for query performance

### 03 — Aggregation (Gold Layer)
- **Daily Sales Summary** — revenue, transaction count, success/fail per day
- **Category Summary** — total revenue and avg order value per category
- **User Summary** — total spend, failed attempts, last active date per user
- **Log Summary** — event counts and avg duration by severity per day
- **Error Trends** — error count and avg duration per service per day

### 04 — Reporting
- Overall pipeline KPIs (total revenue, success rate)
- Daily revenue trend
- Top spending users
- Payment failure reason breakdown
- Slow system events (performance tracking)

---

## Automation

### Event-Driven (Real-Time)
```
Upload file to S3 → S3 notifies Lambda → Lambda calls Databricks Jobs API → Pipeline runs
```

### Scheduled
- Runs every 5 hours using Quartz Cron: `0 0 */5 ? * *`

---

## Setup Instructions

### Prerequisites
- Databricks workspace (Free Edition or above)
- AWS account with S3 and Lambda access
- Python 3.x

### 1. Upload Data to S3
Upload `transactions.csv` and `logs.json` from the `data/` folder to your S3 bucket.

### 2. Import Notebooks to Databricks
Go to Databricks → Workspace → Import → upload all `.py` files from `notebooks/` folder.

### 3. Configure AWS Credentials in Notebook 01
```python
AWS_ACCESS_KEY = "your_access_key"
AWS_SECRET_KEY = "your_secret_key"
BUCKET_NAME    = "your-bucket-name"
```

### 4. Run Notebooks in Order
```
01_ingestion → 02_transformation → 03_aggregation → 04_reporting
```

### 5. Create Databricks Job
- Create a job with 4 tasks (one per notebook) in sequence
- Note the Job ID

### 6. Deploy Lambda Function
- Create Lambda function with Python 3.12 runtime
- Paste code from `lambda/lambda_function.py`
- Add environment variables:
  ```
  DATABRICKS_URL   = https://your-workspace.cloud.databricks.com
  DATABRICKS_TOKEN = your_databricks_token
  DATABRICKS_JOB_ID = your_job_id
  ```
- Add S3 trigger: All object create events on your bucket

---

## CI/CD Pipeline (GitHub Actions)

Every `git push` to `main` automatically runs tests and deploys notebooks to Databricks.

```
git push
    ↓
GitHub Actions triggers
    ↓
Job 1: Run Tests         Job 2: Deploy Notebooks
  ├── test_schema.py       (only runs if tests pass)
  └── test_transformation    ├── 01_ingestion → Databricks
        (10 test cases)      ├── 02_transformation → Databricks
                             ├── 03_aggregation → Databricks
                             └── 04_reporting → Databricks
```

### Tests
| File | What it tests |
|------|--------------|
| `tests/test_schema.py` | File existence, required columns, no duplicates, valid status/level values |
| `tests/test_transformation.py` | Null filling, uppercasing, timestamp parsing, slow flag logic |

### How to Set Up CI/CD
1. Go to GitHub repo → **Settings → Secrets and variables → Actions**
2. Add two secrets:

| Secret | Value |
|--------|-------|
| `DATABRICKS_HOST` | `https://your-workspace.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | your Databricks personal access token |

3. Push any change → GitHub Actions runs automatically

---

## Reports Generated

| Report | Description |
|--------|-------------|
| Pipeline Summary | Total revenue, transaction count, success rate |
| Daily Revenue Trend | Revenue per day with avg transaction value |
| Top Revenue Categories | Best performing product categories |
| Top Spending Users | Highest value customers |
| Log Volume by Severity | Daily INFO / WARN / ERROR counts |
| Services with Most Errors | Which services fail the most |
| Slow System Events | Events with response time > 1000ms |
| Payment Failure Reasons | Breakdown of why payments failed |

---

## Author
**Charitha Sree Sakhamuri**  
Data Engineer  
charithasreesakhamuri@gmail.com
