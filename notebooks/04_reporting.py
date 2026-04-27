# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Notebook 4: Reporting
# MAGIC Queries Gold managed tables and produces final business insights.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## Report 1: Overall Pipeline Summary

# COMMAND ----------

df_txn_silver = spark.read.table("pipeline_db.silver_transactions")

total_txns    = df_txn_silver.count()
total_revenue = df_txn_silver.filter(F.col("status") == "COMPLETED").agg(F.sum("amount")).collect()[0][0]
failed_txns   = df_txn_silver.filter(F.col("status") == "FAILED").count()
success_rate  = ((total_txns - failed_txns) / total_txns) * 100

print("=" * 50)
print("         PIPELINE SUMMARY REPORT")
print("=" * 50)
print(f"Total Transactions  : {total_txns}")
print(f"Total Revenue       : ${total_revenue:,.2f}")
print(f"Failed Transactions : {failed_txns}")
print(f"Success Rate        : {success_rate:.1f}%")
print("=" * 50)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Report 2: Daily Revenue Trend

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     txn_date,
# MAGIC     ROUND(total_revenue, 2)         AS total_revenue,
# MAGIC     total_transactions,
# MAGIC     successful_txns,
# MAGIC     failed_txns,
# MAGIC     ROUND(avg_transaction_value, 2) AS avg_txn_value
# MAGIC FROM pipeline_db.gold_daily_sales_summary
# MAGIC ORDER BY txn_date

# COMMAND ----------
# MAGIC %md
# MAGIC ## Report 3: Top Revenue Categories

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     category,
# MAGIC     ROUND(total_revenue, 2)   AS total_revenue,
# MAGIC     total_transactions,
# MAGIC     ROUND(avg_order_value, 2) AS avg_order_value,
# MAGIC     ROUND(highest_order, 2)   AS highest_order
# MAGIC FROM pipeline_db.gold_category_summary
# MAGIC ORDER BY total_revenue DESC

# COMMAND ----------
# MAGIC %md
# MAGIC ## Report 4: Top Spending Users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     user_id,
# MAGIC     ROUND(total_spent, 2) AS total_spent,
# MAGIC     total_transactions,
# MAGIC     failed_attempts,
# MAGIC     unique_categories,
# MAGIC     last_active_date
# MAGIC FROM pipeline_db.gold_user_summary
# MAGIC ORDER BY total_spent DESC
# MAGIC LIMIT 10

# COMMAND ----------
# MAGIC %md
# MAGIC ## Report 5: Daily Log Volume by Severity

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     log_date,
# MAGIC     level,
# MAGIC     event_count,
# MAGIC     ROUND(avg_duration_ms, 1) AS avg_duration_ms,
# MAGIC     max_duration_ms
# MAGIC FROM pipeline_db.gold_log_summary
# MAGIC ORDER BY log_date, level

# COMMAND ----------
# MAGIC %md
# MAGIC ## Report 6: Services with Most Errors

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     service,
# MAGIC     SUM(error_count)                     AS total_errors,
# MAGIC     ROUND(AVG(avg_error_duration_ms), 1) AS avg_error_duration_ms
# MAGIC FROM pipeline_db.gold_error_trends
# MAGIC GROUP BY service
# MAGIC ORDER BY total_errors DESC

# COMMAND ----------
# MAGIC %md
# MAGIC ## Report 7: Slow System Events (Performance Tracking)

# COMMAND ----------

df_logs_silver = spark.read.table("pipeline_db.silver_logs")

df_slow = (
    df_logs_silver
    .filter(F.col("is_slow") == True)
    .select("log_id", "timestamp", "service", "level", "message", "user_id", "duration_ms")
    .orderBy(F.desc("duration_ms"))
)

print(f"Total slow events (>1000ms): {df_slow.count()}")
display(df_slow)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Report 8: Payment Failure Reasons

# COMMAND ----------

df_errors = (
    df_logs_silver
    .filter(F.col("level") == "ERROR")
    .groupBy("message")
    .agg(F.count("log_id").alias("occurrences"))
    .orderBy(F.desc("occurrences"))
)

display(df_errors)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Pipeline Complete
# MAGIC
# MAGIC | Stage | Layer | Tables |
# MAGIC |-------|-------|--------|
# MAGIC | Ingestion | Bronze | `bronze_transactions`, `bronze_logs` |
# MAGIC | Transformation | Silver | `silver_transactions`, `silver_logs` |
# MAGIC | Aggregation | Gold | `gold_daily_sales_summary`, `gold_category_summary`, `gold_user_summary`, `gold_log_summary`, `gold_error_trends` |
# MAGIC | Reporting | — | SQL queries + DataFrames |
