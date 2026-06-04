import os
from dotenv import load_dotenv

load_dotenv()

# Source PostgreSQL
DB_USER     = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_password")
DB_DB       = os.getenv("POSTGRES_DB", "transactions_db")
DB_HOST     = os.getenv("POSTGRES_HOST", "postgres_source")
DB_PORT     = os.getenv("POSTGRES_PORT", "5432")

# BigQuery target
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "e2e-data-pipeline-497509")
BQ_DATASET     = os.getenv("BQ_DATASET_BRONZE", "bronze_transactions")
BQ_TABLE       = os.getenv("BQ_TABLE", "transactions_raw")
SOURCE_NAME    = os.getenv("SOURCE_NAME", "postgres_transactions")
