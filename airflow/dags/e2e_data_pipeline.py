from datetime import datetime, timedelta
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# ============================================================
# NTT E2E Data Pipeline DAG
# ============================================================
# ARCHITECTURE:
#   start → local batch ingestion (PostgreSQL→BQ) → dbt deps
#         → dbt bronze → dbt silver → dbt snapshot → dbt gold → end
#
# WHY LOCAL INGESTION HERE?
#   The ingestion code is still packaged as a standalone Python module/container,
#   but the local development path lets Airflow execute that module directly.
#   Airflow connects to the Docker Compose source Postgres service, then writes
#   the extracted batch into BigQuery using mounted gcloud ADC credentials.
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

with DAG(
    'ntt_e2e_data_pipeline',
    default_args=default_args,
    description='NTT E2E Pipeline: Ingestion → dbt Bronze → Silver (Star Schema) → Snapshot → Gold',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['ntt', 'gcp', 'dbt', 'star-schema'],
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

    # ── STEP 1: Batch Ingestion (local Postgres -> BigQuery Bronze) ───────
    trigger_ingestion = BashOperator(
        task_id='trigger_batch_ingestion',
        bash_command=(
            'cd /opt/airflow/project && '
            'python ingestion/src/main.py'
        ),
        env={
            'POSTGRES_HOST': 'postgres_source',
            'POSTGRES_PORT': '5432',
            'POSTGRES_USER': 'postgres',
            'POSTGRES_PASSWORD': 'postgres_password',
            'POSTGRES_DB': 'transactions_db',
            'GCP_PROJECT_ID': 'e2e-data-pipeline-497509',
            'BQ_DATASET_BRONZE': 'bronze_transactions',
            'TABLES_CONFIG_PATH': '/opt/airflow/project/ingestion/src/tables_config.json',
            'GOOGLE_APPLICATION_CREDENTIALS': '/home/airflow/.config/gcloud/application_default_credentials.json',
        },
    )


    # ── STEP 1.5: Copy dbt project to /tmp (VirtioFS workaround) ───────────
    copy_dbt_project = BashOperator(
        task_id='copy_dbt_project',
        bash_command='rm -rf /tmp/dbt_pipeline && cp -r /opt/airflow/project/dbt_pipeline /tmp/dbt_pipeline'
    )

    # ── STEP 2: Install dbt packages (dbt_utils for surrogate keys) ────────
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=(
            f'dbt deps '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR}'
        ),
    )

    # ── STEP 3: dbt Bronze — stg_grocery_transactions ──────────────────────
    dbt_run_bronze = BashOperator(
        task_id='dbt_run_bronze',
        bash_command=(
            f'dbt run '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--select models/bronze'
        ),
    )

    # ── STEP 4: dbt Silver — Star Schema ───────────────────────────────────
    # dim_customers, dim_stores, dim_products → fact_grocery_transactions
    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command=(
            f'dbt run '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--select models/silver'
        ),
    )

    # ── STEP 5: SCD Type 2 snapshot — scd_customers ────────────────────────
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot_scd',
        bash_command=(
            f'dbt snapshot '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR}'
        ),
    )

    # ── STEP 6: dbt Gold — mart_sales_summary (VIEW for Looker Studio) ─────
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

    # ── DAG Flow ────────────────────────────────────────────────────────────
    (
        start_pipeline
        >> trigger_ingestion
        >> copy_dbt_project
        >> dbt_deps
        >> dbt_run_bronze
        >> dbt_run_silver
        >> dbt_snapshot
        >> dbt_run_gold
        >> end_pipeline
    )
