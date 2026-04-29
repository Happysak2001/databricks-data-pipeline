import pytest
import pandas as pd
import os

TRANSACTIONS_PATH = "data/transactions.csv"
LOGS_PATH         = "data/logs.json"

EXPECTED_TRANSACTION_COLS = {
    "transaction_id", "user_id", "amount",
    "category", "status", "timestamp", "merchant"
}

EXPECTED_LOG_COLS = {
    "log_id", "timestamp", "level",
    "service", "message", "user_id", "duration_ms"
}


# ---------- File Existence ----------

def test_transactions_file_exists():
    assert os.path.exists(TRANSACTIONS_PATH), f"transactions.csv not found at {TRANSACTIONS_PATH}"


def test_logs_file_exists():
    assert os.path.exists(LOGS_PATH), f"logs.json not found at {LOGS_PATH}"


# ---------- Schema Validation ----------

def test_transactions_has_required_columns():
    df = pd.read_csv(TRANSACTIONS_PATH)
    missing = EXPECTED_TRANSACTION_COLS - set(df.columns)
    assert not missing, f"Missing columns in transactions.csv: {missing}"


def test_logs_has_required_columns():
    df = pd.read_json(LOGS_PATH, lines=True)
    missing = EXPECTED_LOG_COLS - set(df.columns)
    assert not missing, f"Missing columns in logs.json: {missing}"


# ---------- Data Quality ----------

def test_transactions_not_empty():
    df = pd.read_csv(TRANSACTIONS_PATH)
    assert len(df) > 0, "transactions.csv is empty"


def test_logs_not_empty():
    df = pd.read_json(LOGS_PATH, lines=True)
    assert len(df) > 0, "logs.json is empty"


def test_transactions_has_no_duplicate_ids():
    df = pd.read_csv(TRANSACTIONS_PATH)
    assert df["transaction_id"].nunique() == len(df), "Duplicate transaction_ids found"


def test_logs_has_no_duplicate_ids():
    df = pd.read_json(LOGS_PATH, lines=True)
    assert df["log_id"].nunique() == len(df), "Duplicate log_ids found"


def test_transactions_status_values():
    df = pd.read_csv(TRANSACTIONS_PATH)
    valid_statuses = {"completed", "failed"}
    actual = set(df["status"].dropna().str.strip().str.lower().unique())
    invalid = actual - valid_statuses
    assert not invalid, f"Invalid status values found: {invalid}"


def test_logs_level_values():
    df = pd.read_json(LOGS_PATH, lines=True)
    valid_levels = {"INFO", "WARN", "ERROR"}
    actual = set(df["level"].dropna().str.strip().str.upper().unique())
    invalid = actual - valid_levels
    assert not invalid, f"Invalid log level values found: {invalid}"
