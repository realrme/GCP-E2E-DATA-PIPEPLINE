import os
import json
import logging
import uuid
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
# NTT E2E Data Pipeline DAG — with OpenLineage lineage tracking
# ============================================================
# ARCHITECTURE:
#   start
#     → batch_ingest_postgres_to_bq   ← @task PythonOperator
#         Reads: postgres_source:5432  (local Docker service)
#         Writes: BigQuery bronze_transactions.raw_grocery_transactions
#         Emits: OpenLineage START/COMPLETE events → GCP Dataplex Lineage API
#     → copy_dbt_project
#     → dbt_deps    (dbt-ol: emits lineage for package resolution)
#     → dbt_run_bronze   (dbt-ol: emits Bronze model lineage)
#     → dbt_run_silver   (dbt-ol: emits Silver model lineage)
#     → dbt_snapshot_scd (dbt-ol: emits Snapshot lineage)
#     → dbt_run_gold     (dbt-ol: emits Gold model lineage)
#     → end
# ============================================================

DBT_PROJECT_DIR  = '/tmp/dbt_pipeline'
DBT_PROFILES_DIR = '/tmp/dbt_pipeline'

# ── OpenLineage config ─────────────────────────────────────────────────────────
OL_NAMESPACE  = os.environ.get('OPENLINEAGE_NAMESPACE', 'ntt-e2e-pipeline')
GCP_PROJECT   = os.environ.get('GCP_PROJECT_ID', 'e2e-data-pipeline-497509')
BQ_DATASET_BZ = os.environ.get('BQ_DATASET_BRONZE', 'bronze_transactions')

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
    bq_table   = f"{GCP_PROJECT}.{BQ_DATASET_BZ}.{dest_table}"
    client     = bigquery.Client(project=GCP_PROJECT)
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


# ── OpenLineage helpers ────────────────────────────────────────────────────────

def _ol_emit(run_id: str, job_name: str, event_type: str,
             inputs: list, outputs: list) -> None:
    """
    Emit a single OpenLineage RunEvent to the configured transport.

    Reads OPENLINEAGE_CONFIG from the environment (set in docker-compose.yml).
    Falls back to a no-op if openlineage-python is not installed, so the 
    pipeline continues on any lineage failure.
    """
    try:
        from openlineage.client import OpenLineageClient
        from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
        import google.auth
        import google.auth.transport.requests

        # Fetch valid short-lived GCP token
        credentials, _ = google.auth.default()
        req = google.auth.transport.requests.Request()
        credentials.refresh(req)

        # Inject into environment for OpenLineageClient HTTP transport
        os.environ['OPENLINEAGE_URL'] = "https://datalineage.googleapis.com"
        os.environ['OPENLINEAGE_ENDPOINT'] = "v1/projects/e2e-data-pipeline-497509/locations/us-central1:processOpenLineageRunEvent"
        os.environ['OPENLINEAGE_API_KEY'] = credentials.token

        # client natively uses api_key auth if OPENLINEAGE_API_KEY is present
        client = OpenLineageClient.from_environment()

        state = {
            "START":    RunState.START,
            "COMPLETE": RunState.COMPLETE,
            "FAIL":     RunState.FAIL,
        }.get(event_type, RunState.OTHER)

        now = datetime.now(timezone.utc).isoformat()

        event = RunEvent(
            eventType=state,
            eventTime=now,
            run=Run(runId=run_id),
            job=Job(namespace=OL_NAMESPACE, name=job_name),
            inputs=[Dataset(namespace=ns, name=name) for ns, name in inputs],
            outputs=[Dataset(namespace=ns, name=name) for ns, name in outputs],
            producer="https://github.com/realrme/GCP-E2E-DATA-PIPEPLINE",
        )
        client.emit(event)
        logging.info("[openlineage] Emitted %s event for job '%s'", event_type, job_name)
    except Exception as exc:
        # Lineage emission must never break the actual pipeline
        logging.warning("[openlineage] Emit failed (non-fatal): %s", exc)


# ── DAG Definition ─────────────────────────────────────────────────────────────

with DAG(
    'ntt_e2e_data_pipeline',
    default_args=default_args,
    description='NTT E2E Pipeline: Ingestion (Postgres→BQ) → dbt Bronze → Silver (Star Schema) → Snapshot → Gold',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['ntt', 'gcp', 'dbt', 'star-schema', 'openlineage'],
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

    # ── STEP 1: Batch Ingestion — local Postgres → BigQuery Bronze ─────────────
    @task(task_id='batch_ingest_postgres_to_bq')
    def batch_ingest_postgres_to_bq():
        """
        Airflow-native batch ingestion task with OpenLineage lineage emission.

        Pulls rows from every table listed in tables_config.json out of the
        local Docker 'postgres_source' service, stamps metadata, and loads
        them into the BigQuery bronze dataset.

        OpenLineage events are emitted at START and COMPLETE so the Dataplex
        Lineage API records the lineage edge:
          postgres://postgres_source → bigquery://bronze_transactions.*
        """
        log = logging.getLogger(__name__)
        log.info("=== Batch Ingestion START ===")

        default_cfg = '/opt/airflow/project/ingestion/src/tables_config.json'
        config_path = os.environ.get('TABLES_CONFIG_PATH', default_cfg)
        log.info("Loading table config from: %s", config_path)

        with open(config_path, 'r') as fh:
            tables = json.load(fh)

        pg_host = os.environ.get('POSTGRES_HOST', 'postgres_source')
        pg_port = os.environ.get('POSTGRES_PORT', '5432')
        pg_db   = os.environ.get('POSTGRES_DB',   'transactions_db')
        engine  = _get_pg_engine()

        for cfg in tables:
            src           = cfg['src']
            dest          = cfg['dest']
            partition_col = cfg.get('partition_col')

            # ── Declare lineage: Postgres table → BQ table ──────────────────
            run_id     = str(uuid.uuid4())
            job_name   = f"batch_ingest.{src}"
            pg_ns      = f"postgres://{pg_host}:{pg_port}"
            bq_ns      = f"bigquery://{GCP_PROJECT}"
            ol_inputs  = [(pg_ns, f"{pg_db}.{src}")]
            ol_outputs = [(bq_ns, f"{BQ_DATASET_BZ}.{dest}")]

            _ol_emit(run_id, job_name, "START", ol_inputs, ol_outputs)

            try:
                log.info("Ingesting: %s → %s", src, dest)
                df = _extract(engine, src)
                df = _add_metadata(df, src, partition_col)
                _load_to_bq(df, dest, partition_col)
                _ol_emit(run_id, job_name, "COMPLETE", ol_inputs, ol_outputs)
            except Exception as exc:
                _ol_emit(run_id, job_name, "FAIL", ol_inputs, ol_outputs)
                raise

        log.info("=== Batch Ingestion SUCCESS ===")

    ingest_task = batch_ingest_postgres_to_bq()

    # ── STEP 1.5: Copy dbt project to /tmp (VirtioFS workaround) ──────────────
    copy_dbt_project = BashOperator(
        task_id='copy_dbt_project',
        bash_command='rm -rf /tmp/dbt_pipeline && cp -r /opt/airflow/project/dbt_pipeline /tmp/dbt_pipeline',
    )

    dbt_ol_prefix = (
        'export OPENLINEAGE_URL="https://datalineage.googleapis.com" && '
        'export OPENLINEAGE_ENDPOINT="v1/projects/e2e-data-pipeline-497509/locations/us-central1:processOpenLineageRunEvent" && '
        'export OPENLINEAGE_API_KEY="$(python -c \'import google.auth, google.auth.transport.requests; creds, _ = google.auth.default(); creds.refresh(google.auth.transport.requests.Request()); print(creds.token)\')" && '
    )

    # ── STEP 2: Install dbt packages ───────────────────────────────────────────
    # dbt-ol wraps dbt deps and emits an OpenLineage event on completion.
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=(
            f'{dbt_ol_prefix} '
            f'dbt-ol deps '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR}'
        ),
    )

    # ── STEP 3: dbt Bronze — stg_grocery_transactions ─────────────────────────
    # dbt-ol emits lineage: source(bronze.raw_grocery_transactions) → stg_grocery_transactions
    dbt_run_bronze = BashOperator(
        task_id='dbt_run_bronze',
        bash_command=(
            f'{dbt_ol_prefix} '
            f'dbt-ol run '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--select models/bronze'
        ),
    )

    # ── STEP 4: dbt Silver — Star Schema ──────────────────────────────────────
    # dbt-ol emits lineage: stg_grocery_transactions → dim_* + fact_*
    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command=(
            f'{dbt_ol_prefix} '
            f'dbt-ol run '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--select models/silver'
        ),
    )

    # ── STEP 5: SCD Type 2 snapshot — scd_customers ───────────────────────────
    # dbt-ol emits lineage: dim_customers → scd_customers
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot_scd',
        bash_command=(
            f'{dbt_ol_prefix} '
            f'dbt-ol snapshot '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR}'
        ),
    )

    # ── STEP 6: dbt Gold — mart_sales_summary ─────────────────────────────────
    # dbt-ol emits lineage: fact_grocery_transactions → mart_sales_summary
    dbt_run_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command=(
            f'{dbt_ol_prefix} '
            f'dbt-ol run '
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
