import os
from dotenv import load_dotenv

# Load env variables if .env file exists (mostly for local development)
load_dotenv()

# Source Database Config
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_password")
DB_DB = os.getenv("POSTGRES_DB", "transactions_db")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

# GCP Target Config
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
BQ_DATASET = os.getenv("BQ_DATASET_BRONZE", "bronze_transactions")
BQ_TABLE = os.getenv("BQ_TABLE_TRANSACTIONS", "transactions_raw")
