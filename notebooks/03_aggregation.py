# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Notebook 3: Aggregation — Gold Layer
# MAGIC Builds aggregated summary tables from Silver managed tables.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Read Silver Tables

# COMMAND ----------

df_txn  = spark.read.table("pipeline_db.silver_transactions")
df_logs = spark.read.table("pipeline_db.silver_logs")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Daily Sales Summary

# COMMAND ----------

df_daily_sales = (
    df_txn
    .groupBy("txn_date")
    .agg(
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount")).otherwise(0))
         .alias("total_revenue"),
        F.count("transaction_id").alias("total_transactions"),
        F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("successful_txns"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_txns"),
        F.avg(F.when(F.col("status") == "COMPLETED", F.col("amount"))).alias("avg_transaction_value")
    )
    .orderBy("txn_date")
)

display(df_daily_sales)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Category-Level Summary

# COMMAND ----------

df_category_summary = (
    df_txn
    .filter(F.col("status") == "COMPLETED")
    .groupBy("category")
    .agg(
        F.sum("amount").alias("total_revenue"),
        F.count("transaction_id").alias("total_transactions"),
        F.avg("amount").alias("avg_order_value"),
        F.max("amount").alias("highest_order"),
        F.min("amount").alias("lowest_order")
    )
    .orderBy(F.desc("total_revenue"))
)

display(df_category_summary)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: User-Level Summary

# COMMAND ----------

df_user_summary = (
    df_txn
    .groupBy("user_id")
    .agg(
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount")).otherwise(0))
         .alias("total_spent"),
        F.count("transaction_id").alias("total_transactions"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_attempts"),
        F.countDistinct("category").alias("unique_categories"),
        F.max("txn_date").alias("last_active_date")
    )
    .orderBy(F.desc("total_spent"))
)

display(df_user_summary)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: Log Volume Summary

# COMMAND ----------

df_log_summary = (
    df_logs
    .groupBy("log_date", "level")
    .agg(
        F.count("log_id").alias("event_count"),
        F.avg("duration_ms").alias("avg_duration_ms"),
        F.max("duration_ms").alias("max_duration_ms")
    )
    .orderBy("log_date", "level")
)

display(df_log_summary)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6: Error Trends by Service

# COMMAND ----------

df_error_trends = (
    df_logs
    .filter(F.col("level") == "ERROR")
    .groupBy("service", "log_date")
    .agg(
        F.count("log_id").alias("error_count"),
        F.avg("duration_ms").alias("avg_error_duration_ms")
    )
    .orderBy("log_date", F.desc("error_count"))
)

display(df_error_trends)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 7: Write Gold Managed Tables

# COMMAND ----------

def write_gold(df, table_name):
    df.write.format("delta").mode("overwrite").saveAsTable(f"pipeline_db.{table_name}")
    print(f"Gold table written: pipeline_db.{table_name}")

write_gold(df_daily_sales,      "gold_daily_sales_summary")
write_gold(df_category_summary, "gold_category_summary")
write_gold(df_user_summary,     "gold_user_summary")
write_gold(df_log_summary,      "gold_log_summary")
write_gold(df_error_trends,     "gold_error_trends")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Layer Complete
# MAGIC Aggregated managed tables ready:
# MAGIC - `pipeline_db.gold_daily_sales_summary`
# MAGIC - `pipeline_db.gold_category_summary`
# MAGIC - `pipeline_db.gold_user_summary`
# MAGIC - `pipeline_db.gold_log_summary`
# MAGIC - `pipeline_db.gold_error_trends`
# MAGIC
# MAGIC Proceed to **Notebook 4: Reporting**
