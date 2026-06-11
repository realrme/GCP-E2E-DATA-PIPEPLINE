import json
import logging
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Cloud SQL connection details
DB_USER = "postgres"
DB_PASSWORD = "postgres_password"
DB_DB = "transactions_db"
INSTANCE_CONNECTION_NAME = "e2e-data-pipeline-497509:us-central1:transactions-db-instance"
TABLE_NAME = "grocery_transactions"
JSON_FILE = "data/grocery_chain_data.json"

from google.cloud.sql.connector import Connector
import pg8000

def getconn():
    connector = Connector()
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_DB,
        ip_type="public"
    )
    return conn

def main():
    log.info(f"Connecting to Cloud SQL via Connector at {INSTANCE_CONNECTION_NAME}...")
    engine = create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )

    log.info(f"Reading JSON file from {JSON_FILE}...")
    try:
        with open(JSON_FILE, "r") as f:
            data = json.load(f)
        
        df = pd.DataFrame(data)
        log.info(f"Loaded {len(df)} records into memory.")

        log.info(f"Writing to PostgreSQL table '{TABLE_NAME}'...")
        df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
        log.info(f"Successfully seeded {len(df)} records to Cloud SQL!")
    except Exception as e:
        log.error(f"Seeding failed: {e}")

if __name__ == "__main__":
    main()
