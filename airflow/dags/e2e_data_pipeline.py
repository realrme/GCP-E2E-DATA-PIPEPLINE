from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Default arguments for the DAG
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
    description='Orchestrator DAG for NTT End-to-End Data Pipeline (Ingestion -> dbt)',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['ntt', 'gcp', 'dbt'],
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

    # Task 1: Trigger Batch Ingestion Job on GCP Cloud Run
    # (Using BashOperator skeleton; in production, use CloudRunExecuteJobOperator or HTTPOperator)
    trigger_ingestion = BashOperator(
        task_id='trigger_batch_ingestion',
        bash_command='echo "Executing Cloud Run batch ingestion job..."',
    )

    # Task 2: Run dbt transformation (Bronze/Staging)
    # (Using BashOperator skeleton; runs dbt run against bronze model)
    dbt_run_bronze = BashOperator(
        task_id='dbt_run_bronze',
        bash_command='echo "dbt run --select models/bronze"',
    )

    # Task 3: Run dbt transformation (Silver/Normalize)
    # (Using BashOperator skeleton; runs dbt run against silver model)
    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command='echo "dbt run --select models/silver"',
    )

    # Task 4: Run dbt transformation (Gold/Aggregate)
    # (Using BashOperator skeleton; runs dbt run against gold model)
    dbt_run_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command='echo "dbt run --select models/gold"',
    )

    end_pipeline = EmptyOperator(task_id='end_pipeline')

    # DAG Task Dependency flow
    start_pipeline >> trigger_ingestion >> dbt_run_bronze >> dbt_run_silver >> dbt_run_gold >> end_pipeline
