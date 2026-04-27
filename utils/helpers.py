# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Helpers / Utility Functions
# MAGIC Reusable functions used across pipeline notebooks.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def log_row_count(df: DataFrame, label: str) -> None:
    print(f"[{label}] Row count: {df.count()}")


def drop_duplicates_on(df: DataFrame, key_cols: list) -> DataFrame:
    before = df.count()
    df_deduped = df.dropDuplicates(key_cols)
    after = df_deduped.count()
    print(f"Duplicates removed: {before - after}")
    return df_deduped


def null_summary(df: DataFrame) -> DataFrame:
    total = df.count()
    null_counts = [
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]
    return df.agg(*null_counts)


def write_delta(df: DataFrame, path: str, partition_col: str = None, mode: str = "overwrite") -> None:
    writer = df.write.format("delta").mode(mode)
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.save(path)
    print(f"Written to Delta: {path}")


def register_table(spark, db: str, table: str, path: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db}.{table}
        USING DELTA
        LOCATION '{path}'
    """)
    print(f"Registered table: {db}.{table}")
