# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Notebook 1: Data Ingestion — Bronze Layer
# MAGIC Reads the file that just landed in S3 (passed by Lambda as parameters).
# MAGIC Detects file type and writes to the appropriate Bronze Delta table.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Read Job Parameters

# COMMAND ----------

# Parameters passed by Lambda via Databricks Jobs API
# When running manually, defaults are used
dbutils.widgets.text("bucket_name", "charitha-db",       "S3 Bucket Name")
dbutils.widgets.text("file_key",    "transactions.csv",  "S3 File Key")

BUCKET_NAME = dbutils.widgets.get("bucket_name")
FILE_KEY    = dbutils.widgets.get("file_key")

print(f"Bucket : {BUCKET_NAME}")
print(f"File   : {FILE_KEY}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Configure AWS & Read File from S3

# COMMAND ----------

import boto3
import pandas as pd

# AWS credentials
AWS_ACCESS_KEY = "YOUR_ACCESS_KEY_ID"
AWS_SECRET_KEY = "YOUR_SECRET_ACCESS_KEY"

s3 = boto3.client(
    "s3",
    aws_access_key_id     = AWS_ACCESS_KEY,
    aws_secret_access_key = AWS_SECRET_KEY,
    region_name           = "us-east-1"
)

# Read file from S3
obj = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
print(f"File fetched from S3: s3://{BUCKET_NAME}/{FILE_KEY}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Detect File Type & Parse

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS pipeline_db")

if FILE_KEY.endswith(".csv"):
    # Parse as transactions
    df_pandas = pd.read_csv(obj["Body"])
    df_raw    = spark.createDataFrame(df_pandas)
    target_table = "pipeline_db.bronze_transactions"
    print(f"Detected: CSV → writing to {target_table}")

elif FILE_KEY.endswith(".json"):
    # Parse as logs
    df_pandas = pd.read_json(obj["Body"], lines=True)
    df_raw    = spark.createDataFrame(df_pandas)
    target_table = "pipeline_db.bronze_logs"
    print(f"Detected: JSON → writing to {target_table}")

else:
    raise ValueError(f"Unsupported file type: {FILE_KEY}. Only .csv and .json are supported.")

print(f"Row count: {df_raw.count()}")
df_raw.printSchema()
display(df_raw)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: Write to Bronze Delta Table

# COMMAND ----------

(
    df_raw.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Bronze table written: {target_table}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: Verify

# COMMAND ----------

print(f"-- {target_table} (latest 5 rows) --")
spark.read.table(target_table).show(5)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Layer Complete
# MAGIC File ingested from S3 into Bronze Delta table.
# MAGIC Proceed to **Notebook 2: Transformation**
