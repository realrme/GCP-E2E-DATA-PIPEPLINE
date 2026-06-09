from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# ============================================================
# NTT E2E Data Pipeline DAG
# ============================================================
# ARCHITECTURE FLOW:
#
#  start
#    │
#    ▼
#  trigger_batch_ingestion        ← Python: PostgreSQL → BigQuery (Bronze)
#    │
#    ▼
#  dbt_deps                       ← Install dbt packages (dbt-utils etc.)
#    │
#    ▼
#  dbt_run_bronze                 ← stg_grocery_transactions (type-cast)
#    │
#    ▼
#  dbt_run_silver                 ← dim_customers, dim_stores,
#    │                               dim_products, fact_grocery_transactions
#    │                               (INCREMENTAL — only new partitions)
#    ▼
#  dbt_snapshot                   ← SCD Type 2: scd_customers history
#    │
#    ▼
#  dbt_run_gold                   ← Aggregated views for Looker Studio
#    │
#    ▼
#  end
# ============================================================

DBT_PROJECT_DIR = '/opt/airflow/project/dbt_pipeline'
DBT_PROFILES_DIR = '/opt/airflow/project/dbt_pipeline'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ntt_e2e_data_pipeline',
    default_args=default_args,
    description='Orchestrator DAG for NTT End-to-End Data Pipeline (Ingestion → dbt Bronze → Silver → Snapshot → Gold)',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['ntt', 'gcp', 'dbt', 'star-schema'],
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

    # ──────────────────────────────────────────────────────────
    # STEP 1: Batch Ingestion
    # Reads from PostgreSQL (source DB) and loads to BigQuery
    # (bronze_transactions.raw_grocery_transactions)
    # ──────────────────────────────────────────────────────────
    trigger_ingestion = BashOperator(
        task_id='trigger_batch_ingestion',
        bash_command='python3 /opt/airflow/project/ingestion/src/main.py',
        env={
            'POSTGRES_HOST': 'postgres_source',
            'POSTGRES_PORT': '5432',
            'POSTGRES_USER': 'postgres',
            'POSTGRES_PASSWORD': 'postgres_password',
            'POSTGRES_DB': 'transactions_db',
            'GCP_PROJECT_ID': 'e2e-data-pipeline-497509',
            'BQ_DATASET_BRONZE': 'bronze_transactions',
            'BQ_TABLE': 'raw_grocery_transactions',  # ← updated table name
        }
    )

    # ──────────────────────────────────────────────────────────
    # STEP 2: Install dbt packages
    # Installs packages defined in packages.yml (e.g., dbt_utils)
    # This must run before any dbt run/snapshot commands
    # ──────────────────────────────────────────────────────────
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=(
            f'dbt deps '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR}'
        ),
    )

    # ──────────────────────────────────────────────────────────
    # STEP 3: dbt Bronze Layer
    # Model: stg_grocery_transactions
    # Casts raw BigQuery types → clean typed columns
    # Materialization: table (full rebuild, thin wrapper)
    # ──────────────────────────────────────────────────────────
    dbt_run_bronze = BashOperator(
        task_id='dbt_run_bronze',
        bash_command=(
            f'dbt run '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--select models/bronze'
        ),
    )

    # ──────────────────────────────────────────────────────────
    # STEP 4: dbt Silver Layer (Star Schema)
    # Models:
    #   - dim_customers  → one row per unique customer
    #   - dim_stores     → one row per unique store
    #   - dim_products   → one row per unique product+aisle
    #   - fact_grocery_transactions → INCREMENTAL fact table
    #
    # WHY INCREMENTAL?
    #   The fact table grows daily. We only process NEW rows
    #   (WHERE ingestion_date > MAX(ingestion_date) in the table)
    #   to avoid reprocessing all historical data every run.
    # ──────────────────────────────────────────────────────────
    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command=(
            f'dbt run '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--select models/silver'
        ),
    )

    # ──────────────────────────────────────────────────────────
    # STEP 5: dbt Snapshot (SCD Type 2)
    # Snapshot: scd_customers
    # Tracks loyalty_points changes over time.
    # Each change creates a new row; old rows get dbt_valid_to set.
    #
    # WHY AFTER SILVER?
    #   The snapshot reads from dim_customers (a silver model),
    #   so silver must run first to have the latest state.
    # ──────────────────────────────────────────────────────────
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot_scd',
        bash_command=(
            f'dbt snapshot '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR}'
        ),
    )

    # ──────────────────────────────────────────────────────────
    # STEP 6: dbt Gold Layer
    # Aggregated views and summary tables for Looker Studio.
    # Built on top of Silver (reads from fact + dims).
    # Materialization: view (no storage cost, always fresh)
    # ──────────────────────────────────────────────────────────
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

    # ──────────────────────────────────────────────────────────
    # DAG Task Dependency Flow
    # Each >> means "must complete successfully before next runs"
    # ──────────────────────────────────────────────────────────
    (
        start_pipeline
        >> trigger_ingestion
        >> dbt_deps
        >> dbt_run_bronze
        >> dbt_run_silver
        >> dbt_snapshot
        >> dbt_run_gold
        >> end_pipeline
    )
