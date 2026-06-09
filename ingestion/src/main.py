import os
import sys
import json
import logging
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery

from config import (
    DB_USER, DB_PASSWORD, DB_DB, DB_HOST, DB_PORT,
    GCP_PROJECT_ID, BQ_DATASET
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
def extract_from_postgres(table_name: str) -> pd.DataFrame:
    """
    Extracts all records from the specified table in PostgreSQL.
    Returns a pandas DataFrame.
    """
    if DB_HOST.startswith("/"):
        # Unix socket connection
        db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@/{DB_DB}?host={DB_HOST}"
    else:
        # TCP connection
        db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DB}"

    engine = create_engine(db_url)
    query  = f'SELECT * FROM "{table_name}"'

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    log.info(f"[{table_name}] Extracted {len(df)} rows from source.")
    return df

# ---------------------------------------------------------------------------
# Add Metadata
# ---------------------------------------------------------------------------
def add_metadata(df: pd.DataFrame, table_name: str, partition_col: str = None) -> pd.DataFrame:
    """
    Adds system metadata columns to the DataFrame.
    """
    now = datetime.now(timezone.utc)
    df["_ingested_at"] = now
    df["_source_file"] = f"postgres_table:{table_name}"
    
    if partition_col == "ingestion_date":
        df["ingestion_date"] = now.date()
        
    return df

# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
def load_to_bigquery(df: pd.DataFrame, dest_table: str, partition_col: str = None) -> None:
    """
    Loads DataFrame to BigQuery table.
    - If it's the orders table, we WRITE_APPEND and partition.
    - For other tables, we WRITE_TRUNCATE to refresh full dim/mapping data.
    """
    bq_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{dest_table}"
    log.info(f"[{dest_table}] Loading {len(df)} rows → {bq_table_id}")

    client = bigquery.Client(project=GCP_PROJECT_ID)

    if dest_table == "raw_orders":
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    else:
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True,
    )

    if partition_col:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_col,
        )

    job = client.load_table_from_dataframe(df, bq_table_id, job_config=job_config)
    job.result()  # Wait for completion

    table = client.get_table(bq_table_id)
    log.info(f"[{dest_table}] Load complete. Total rows in table: {table.num_rows}")

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main():
    log.info("=== Batch Ingestion Microservice — START ===")
    
    # Load configuration dynamically
    default_config_path = os.path.join(os.path.dirname(__file__), "tables_config.json")
    config_path = os.getenv("TABLES_CONFIG_PATH", default_config_path)
    log.info(f"Loading table config from: {config_path}")
    
    try:
        with open(config_path, "r") as f:
            tables_to_ingest = json.load(f)
    except Exception as e:
        log.error(f"Failed to load table configuration from {config_path}: {e}")
        sys.exit(1)

    try:
        for config in tables_to_ingest:
            src = config["src"]
            dest = config["dest"]
            partition_col = config.get("partition_col")
            
            log.info(f"Starting ingestion for {src} -> {dest}...")
            df = extract_from_postgres(src)
            df = add_metadata(df, src, partition_col)
            load_to_bigquery(df, dest, partition_col)
            
        log.info("=== Batch Ingestion Microservice — SUCCESS ===")
    except Exception as e:
        log.error(f"Ingestion failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
