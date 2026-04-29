# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Notebook 2: Transformation — Silver Layer
# MAGIC Reads from Bronze managed tables, cleans and standardizes data, writes to Silver managed tables.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

print("Pipeline v2 — Transformation started.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Read Bronze Tables

# COMMAND ----------

df_txn  = spark.read.table("pipeline_db.bronze_transactions")
df_logs = spark.read.table("pipeline_db.bronze_logs")

print(f"Raw transactions: {df_txn.count()} rows")
print(f"Raw logs:         {df_logs.count()} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Clean Transactions

# COMMAND ----------

# Cast amount to Double (handles None as null)
df_txn_clean = df_txn.withColumn("amount", F.col("amount").cast(DoubleType()))

# Drop rows where critical fields are null
df_txn_clean = df_txn_clean.dropna(subset=["transaction_id", "user_id", "timestamp"])

# Fill failed transactions with amount = 0
df_txn_clean = df_txn_clean.fillna({"amount": 0.0, "category": "UNKNOWN"})

# Standardize category and status to UPPERCASE
df_txn_clean = df_txn_clean.withColumn("category", F.upper(F.trim(F.col("category"))))
df_txn_clean = df_txn_clean.withColumn("status",   F.upper(F.trim(F.col("status"))))

# Parse timestamp string to TimestampType
df_txn_clean = df_txn_clean.withColumn(
    "timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)

# Extract date column for partitioning
df_txn_clean = df_txn_clean.withColumn("txn_date", F.to_date(F.col("timestamp")))

# Add ingestion metadata
df_txn_clean = df_txn_clean.withColumn("ingested_at", F.current_timestamp())

print(f"Clean transactions: {df_txn_clean.count()} rows")
display(df_txn_clean)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Validate Transactions

# COMMAND ----------

null_amounts   = df_txn_clean.filter(F.col("amount").isNull()).count()
failed_count   = df_txn_clean.filter(F.col("status") == "FAILED").count()
complete_count = df_txn_clean.filter(F.col("status") == "COMPLETED").count()

print(f"Null amounts after fill : {null_amounts}")
print(f"Failed transactions     : {failed_count}")
print(f"Completed transactions  : {complete_count}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: Clean Logs

# COMMAND ----------

# Drop rows with null log_id or timestamp
df_logs_clean = df_logs.dropna(subset=["log_id", "timestamp"])

# Parse timestamp
df_logs_clean = df_logs_clean.withColumn(
    "timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)

# Standardize log level to UPPERCASE
df_logs_clean = df_logs_clean.withColumn("level", F.upper(F.trim(F.col("level"))))

# Fill null user_id as SYSTEM for system-generated logs
df_logs_clean = df_logs_clean.fillna({"user_id": "SYSTEM"})

# Flag high-latency logs (duration > 1000ms is anomalous)
df_logs_clean = df_logs_clean.withColumn(
    "is_slow", F.when(F.col("duration_ms") > 1000, True).otherwise(False)
)

# Extract log date for partitioning
df_logs_clean = df_logs_clean.withColumn("log_date", F.to_date(F.col("timestamp")))

# Add ingestion metadata
df_logs_clean = df_logs_clean.withColumn("ingested_at", F.current_timestamp())

print(f"Clean logs: {df_logs_clean.count()} rows")
display(df_logs_clean)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: Validate Logs

# COMMAND ----------

error_count = df_logs_clean.filter(F.col("level") == "ERROR").count()
warn_count  = df_logs_clean.filter(F.col("level") == "WARN").count()
slow_count  = df_logs_clean.filter(F.col("is_slow") == True).count()

print(f"ERROR logs  : {error_count}")
print(f"WARN logs   : {warn_count}")
print(f"Slow events : {slow_count}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6: Write Silver Managed Tables (partitioned by date)

# COMMAND ----------

(
    df_txn_clean.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("txn_date")
    .saveAsTable("pipeline_db.silver_transactions")
)
print("Silver transactions written.")

(
    df_logs_clean.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("log_date")
    .saveAsTable("pipeline_db.silver_logs")
)
print("Silver logs written.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Layer Complete
# MAGIC Clean, validated, partitioned managed tables ready:
# MAGIC - `pipeline_db.silver_transactions`
# MAGIC - `pipeline_db.silver_logs`
# MAGIC
# MAGIC Proceed to **Notebook 3: Aggregation**
