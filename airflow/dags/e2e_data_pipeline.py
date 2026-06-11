import os
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

# ============================================================
# NTT E2E Data Pipeline DAG
# ============================================================
# ARCHITECTURE:
#   start
#     → batch_ingest_postgres_to_bq   ← PythonOperator (@task)
#         Reads: postgres_source:5432  (local Docker service)
#         Writes: BigQuery bronze_transactions.raw_grocery_transactions
#         Auth:   ADC via mounted ~/.config/gcloud
#     → copy_dbt_project
#     → dbt_deps → dbt_run_bronze → dbt_run_silver → dbt_snapshot → dbt_run_gold
#     → end
# ============================================================

DBT_PROJECT_DIR  = '/tmp/dbt_pipeline'
DBT_PROFILES_DIR = '/tmp/dbt_pipeline'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── Ingestion Helpers ──────────────────────────────────────────────────────────

def _get_pg_engine():
    """Build a SQLAlchemy engine pointing to the local Docker postgres_source."""
    host     = os.environ.get('POSTGRES_HOST',     'postgres_source')
    port     = os.environ.get('POSTGRES_PORT',     '5432')
    user     = os.environ.get('POSTGRES_USER',     'postgres')
    password = os.environ.get('POSTGRES_PASSWORD', 'postgres_password')
    db       = os.environ.get('POSTGRES_DB',       'transactions_db')
    url      = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


def _extract(engine, table_name: str) -> pd.DataFrame:
    with engine.connect() as conn:
        df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
    logging.info("[extract] %s → %d rows", table_name, len(df))
    return df


def _add_metadata(df: pd.DataFrame, table_name: str, partition_col: Optional[str]) -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    df['_ingested_at'] = now
    df['_source_file'] = f"postgres_table:{table_name}"
    if partition_col == 'ingestion_date':
        df['ingestion_date'] = now.date()
    return df


def _load_to_bq(df: pd.DataFrame, dest_table: str, partition_col: Optional[str]) -> None:
    project    = os.environ.get('GCP_PROJECT_ID',     'e2e-data-pipeline-497509')
    dataset    = os.environ.get('BQ_DATASET_BRONZE',  'bronze_transactions')
    bq_table   = f"{project}.{dataset}.{dest_table}"

    client     = bigquery.Client(project=project)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )
    if partition_col:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_col,
        )

    job = client.load_table_from_dataframe(df, bq_table, job_config=job_config)
    job.result()

    table = client.get_table(bq_table)
    logging.info("[load] %s → %d total rows in BQ", bq_table, table.num_rows)


# ── DAG Definition ─────────────────────────────────────────────────────────────

with DAG(
    'ntt_e2e_data_pipeline',
    default_args=default_args,
    description='NTT E2E Pipeline: Ingestion (Postgres→BQ) → dbt Bronze → Silver (Star Schema) → Snapshot → Gold',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['ntt', 'gcp', 'dbt', 'star-schema'],
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

    # ── STEP 1: Batch Ingestion — local Postgres → BigQuery Bronze ─────────────
    @task(task_id='batch_ingest_postgres_to_bq')
    def batch_ingest_postgres_to_bq():
        """
        Airflow-native batch ingestion task.

        Pulls rows from every table listed in tables_config.json out of the
        local Docker 'postgres_source' service, stamps metadata, and loads
        them into the BigQuery bronze dataset using Application Default
        Credentials (ADC) mounted at /home/airflow/.config/gcloud.
        """
        log = logging.getLogger(__name__)
        log.info("=== Batch Ingestion START ===")

        # Resolve config file (same path used by the standalone module)
        default_cfg = '/opt/airflow/project/ingestion/src/tables_config.json'
        config_path = os.environ.get('TABLES_CONFIG_PATH', default_cfg)
        log.info("Loading table config from: %s", config_path)

        with open(config_path, 'r') as fh:
            tables = json.load(fh)

        engine = _get_pg_engine()

        for cfg in tables:
            src           = cfg['src']
            dest          = cfg['dest']
            partition_col = cfg.get('partition_col')

            log.info("Ingesting: %s → %s", src, dest)
            df = _extract(engine, src)
            df = _add_metadata(df, src, partition_col)
            _load_to_bq(df, dest, partition_col)

        log.info("=== Batch Ingestion SUCCESS ===")

    ingest_task = batch_ingest_postgres_to_bq()

    # ── STEP 1.5: Copy dbt project to /tmp (VirtioFS workaround) ──────────────
    copy_dbt_project = BashOperator(
        task_id='copy_dbt_project',
        bash_command='rm -rf /tmp/dbt_pipeline && cp -r /opt/airflow/project/dbt_pipeline /tmp/dbt_pipeline',
    )

    # ── STEP 2: Install dbt packages ───────────────────────────────────────────
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=(
            f'dbt deps '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR}'
        ),
    )

    # ── STEP 3: dbt Bronze — stg_grocery_transactions ─────────────────────────
    dbt_run_bronze = BashOperator(
        task_id='dbt_run_bronze',
        bash_command=(
            f'dbt run '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--select models/bronze'
        ),
    )

    # ── STEP 4: dbt Silver — Star Schema ──────────────────────────────────────
    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command=(
            f'dbt run '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--select models/silver'
        ),
    )

    # ── STEP 5: SCD Type 2 snapshot — scd_customers ───────────────────────────
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot_scd',
        bash_command=(
            f'dbt snapshot '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR}'
        ),
    )

    # ── STEP 6: dbt Gold — mart_sales_summary ─────────────────────────────────
    dbt_run_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command=(
            f'dbt run '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--select models/gold'
        ),
    )

    end_pipeline = EmptyOperator(task_id='end_pipeline')

    # ── DAG Flow ───────────────────────────────────────────────────────────────
    (
        start_pipeline
        >> ingest_task
        >> copy_dbt_project
        >> dbt_deps
        >> dbt_run_bronze
        >> dbt_run_silver
        >> dbt_snapshot
        >> dbt_run_gold
        >> end_pipeline
    )
