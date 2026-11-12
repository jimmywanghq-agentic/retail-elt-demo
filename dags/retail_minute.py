"""Airflow coordinates work; it does not contain the business transformation SQL."""
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pipeline.load_orders import load_orders

with DAG(
    dag_id='retail_every_minute',
    # A cron schedule starts a new batch at each minute boundary.
    schedule='* * * * *', start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False, max_active_runs=1,
    default_args={'retries': 1, 'retry_delay': timedelta(seconds=10)},
    tags=['learning', 'retail'],
) as dag:
    load = PythonOperator(task_id='spark_postgres_to_snowflake', python_callable=load_orders)
    transform = BashOperator(
        task_id='dbt_incremental_models_and_tests',
        bash_command='cd /opt/demo/dbt && /home/airflow/dbt-venv/bin/dbt build --profiles-dir . --target demo',
    )
    # dbt runs only after the load succeeds. Its tests can fail the Airflow run.
    load >> transform
