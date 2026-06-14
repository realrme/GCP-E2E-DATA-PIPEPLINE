import os
import json
import logging
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def main():
    # Load environment variables from the root .env file
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    env_path = os.path.join(project_root, ".env")
    load_dotenv(env_path)
    
    # Get config from env
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres_password")
    db = os.getenv("POSTGRES_DB", "transactions_db")
    
    # Check if running in Docker container or on host
    in_docker = os.path.exists("/.dockerenv")
    if in_docker:
        host = os.getenv("POSTGRES_HOST", "postgres_source")
        port = "5432"
    else:
        host = "localhost"
        port = "5433"
        
    db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    log.info(f"Connecting to database: {host}:{port}/{db}")
    
    # Path to JSON file
    json_path = os.path.join(project_root, "local_db", "data", "grocery_chain_data.json")
    log.info(f"Reading JSON data from: {json_path}")
    
    try:
        # Load JSON directly into Pandas DataFrame
        df = pd.read_json(json_path)
        log.info(f"Successfully loaded {len(df)} records from JSON.")
        
        # Connect and seed
        engine = create_engine(db_url)
        table_name = "grocery_transactions"
        log.info(f"Writing data to PostgreSQL table '{table_name}'...")
        
        # Write to Postgres, replace if exists
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        log.info("Database seeding completed successfully!")
        
    except Exception as e:
        log.error(f"Error seeding database: {e}", exc_info=True)
        raise e

if __name__ == "__main__":
    main()
