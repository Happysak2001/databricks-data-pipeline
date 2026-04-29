import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ---------- Transaction Tests ----------

def test_null_amount_filled(spark):
    data = [("TXN001", "USR101", None, "Electronics", "completed", "2024-01-15 10:23:45", "BestBuy")]
    cols = ["transaction_id", "user_id", "amount", "category", "status", "timestamp", "merchant"]
    df = spark.createDataFrame(data, cols)
    df = df.withColumn("amount", F.col("amount").cast(DoubleType()))
    df = df.fillna({"amount": 0.0})
    assert df.filter(F.col("amount").isNull()).count() == 0
    assert df.filter(F.col("amount") == 0.0).count() == 1


def test_status_uppercased(spark):
    data = [("TXN001", "USR101", 100.0, "Electronics", "completed", "2024-01-15 10:23:45", "BestBuy")]
    cols = ["transaction_id", "user_id", "amount", "category", "status", "timestamp", "merchant"]
    df = spark.createDataFrame(data, cols)
    df = df.withColumn("status", F.upper(F.trim(F.col("status"))))
    assert df.filter(F.col("status") == "COMPLETED").count() == 1


def test_category_uppercased(spark):
    data = [("TXN001", "USR101", 100.0, "electronics", "completed", "2024-01-15 10:23:45", "BestBuy")]
    cols = ["transaction_id", "user_id", "amount", "category", "status", "timestamp", "merchant"]
    df = spark.createDataFrame(data, cols)
    df = df.withColumn("category", F.upper(F.trim(F.col("category"))))
    assert df.filter(F.col("category") == "ELECTRONICS").count() == 1


def test_null_transaction_id_dropped(spark):
    data = [
        ("TXN001", "USR101", 100.0, "Electronics", "completed", "2024-01-15 10:23:45", "BestBuy"),
        (None,     "USR102", 50.0,  "Groceries",   "completed", "2024-01-15 11:00:00", "Walmart"),
    ]
    cols = ["transaction_id", "user_id", "amount", "category", "status", "timestamp", "merchant"]
    df = spark.createDataFrame(data, cols)
    df = df.dropna(subset=["transaction_id"])
    assert df.count() == 1


def test_timestamp_parsed(spark):
    data = [("TXN001", "USR101", 100.0, "Electronics", "completed", "2024-01-15 10:23:45", "BestBuy")]
    cols = ["transaction_id", "user_id", "amount", "category", "status", "timestamp", "merchant"]
    df = spark.createDataFrame(data, cols)
    df = df.withColumn("timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    assert df.schema["timestamp"].dataType.typeName() == "timestamp"


def test_txn_date_extracted(spark):
    data = [("TXN001", "USR101", 100.0, "Electronics", "completed", "2024-01-15 10:23:45", "BestBuy")]
    cols = ["transaction_id", "user_id", "amount", "category", "status", "timestamp", "merchant"]
    df = spark.createDataFrame(data, cols)
    df = df.withColumn("timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("txn_date", F.to_date(F.col("timestamp")))
    assert "txn_date" in df.columns
    assert df.filter(F.col("txn_date").isNull()).count() == 0


# ---------- Log Tests ----------

def test_log_null_user_id_filled(spark):
    data = [("LOG001", "2024-01-15 10:00:00", "INFO", "payment-service", "msg", None, 100)]
    cols = ["log_id", "timestamp", "level", "service", "message", "user_id", "duration_ms"]
    df = spark.createDataFrame(data, cols)
    df = df.fillna({"user_id": "SYSTEM"})
    assert df.filter(F.col("user_id") == "SYSTEM").count() == 1


def test_log_level_uppercased(spark):
    data = [("LOG001", "2024-01-15 10:00:00", "info", "payment-service", "msg", "USR101", 100)]
    cols = ["log_id", "timestamp", "level", "service", "message", "user_id", "duration_ms"]
    df = spark.createDataFrame(data, cols)
    df = df.withColumn("level", F.upper(F.trim(F.col("level"))))
    assert df.filter(F.col("level") == "INFO").count() == 1


def test_slow_flag_set_correctly(spark):
    data = [
        ("LOG001", "2024-01-15 10:00:00", "INFO",  "payment-service", "msg", "USR101", 500),
        ("LOG002", "2024-01-15 10:00:00", "ERROR", "payment-service", "msg", "USR102", 2000),
    ]
    cols = ["log_id", "timestamp", "level", "service", "message", "user_id", "duration_ms"]
    df = spark.createDataFrame(data, cols)
    df = df.withColumn("is_slow", F.when(F.col("duration_ms") > 1000, True).otherwise(False))
    assert df.filter(F.col("is_slow") == True).count()  == 1
    assert df.filter(F.col("is_slow") == False).count() == 1


def test_slow_flag_boundary(spark):
    data = [
        ("LOG001", "2024-01-15 10:00:00", "INFO", "payment-service", "msg", "USR101", 1000),
        ("LOG002", "2024-01-15 10:00:00", "INFO", "payment-service", "msg", "USR102", 1001),
    ]
    cols = ["log_id", "timestamp", "level", "service", "message", "user_id", "duration_ms"]
    df = spark.createDataFrame(data, cols)
    df = df.withColumn("is_slow", F.when(F.col("duration_ms") > 1000, True).otherwise(False))
    assert df.filter(F.col("is_slow") == True).count()  == 1   # only 1001
    assert df.filter(F.col("is_slow") == False).count() == 1   # 1000 is NOT slow
