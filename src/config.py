from pathlib import Path


# Project root:
# 07_airflow_sql_ai_pipeline/
PROJECT_ROOT = Path(__file__).resolve().parents[1]


# Data directories
DATA_DIR = PROJECT_ROOT / "data"
INCOMING_DIR = DATA_DIR / "incoming"
PROCESSED_DIR = DATA_DIR / "processed"
QUARANTINE_DIR = DATA_DIR / "quarantine"
GROUND_TRUTH_DIR = DATA_DIR / "ground_truth"


# SQL directory
SQL_DIR = PROJECT_ROOT / "sql"


# Main SQLite database
DB_PATH = PROCESSED_DIR / "ticket_pipeline.db"


# Database schema
SCHEMA_PATH = SQL_DIR / "create_tables.sql"
