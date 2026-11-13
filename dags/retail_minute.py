# This file tells Airflow when to run the pipeline and which task must run first.
"""Airflow coordinates work; it does not contain the business transformation SQL."""
# datetime sets the schedule's starting date; timedelta describes a retry delay; timezone keeps dates in UTC.
from datetime import datetime, timedelta, timezone
# A DAG is Airflow's definition of a workflow: its schedule, tasks, and task order.
from airflow import DAG
# PythonOperator lets an Airflow task call one of our Python functions.
from airflow.operators.python import PythonOperator
# BashOperator lets another task run the dbt command in a Linux shell.
from airflow.operators.bash import BashOperator
# Import the function that reads new PostgreSQL orders with Spark and loads Snowflake.
from pipeline.load_orders import load_orders

# Start the workflow definition; the indented tasks below belong to this workflow.
with DAG(
    # Give the workflow the name you can search for in the Airflow website.
    dag_id='retail_every_minute',
    # Five stars mean every minute, every hour, every day, every month, and every weekday.
    schedule='* * * * *',
    # Make the schedule valid from this UTC date; this does not create old runs because catchup is off.
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    # Skip historical minute slots when the demo starts after being stopped.
    catchup=False,
    # Allow only one pipeline run at a time so two loads cannot overwrite the shared batch table.
    max_active_runs=1,
    # Apply these retry settings to both tasks unless a task supplies its own settings.
    default_args={
        # Retry a failed task once, which helps with a brief connection problem.
        'retries': 1,
        # Wait ten seconds before that retry instead of immediately repeating a failing request.
        'retry_delay': timedelta(seconds=10),
    # Finish the shared retry settings.
    },
    # Add labels so this learning workflow is easy to find in the Airflow website.
    tags=['learning', 'retail'],
# Finish the workflow settings and give this workflow the Python name dag.
) as dag:
    # Define the loading task and keep a reference to it so we can set its execution order.
    load = PythonOperator(
        # Use a task name that explains its source, processing tool, and destination in the job graph.
        task_id='spark_postgres_to_snowflake',
        # Pass the function itself; Airflow calls it later when this task runs.
        python_callable=load_orders,
    # Finish the loading-task definition.
    )
    # Define the task that turns loaded RAW orders into tested reporting tables.
    transform = BashOperator(
        # Give the transformation task a clear name in the job graph and logs.
        task_id='dbt_incremental_models_and_tests',
        # Enter the dbt project, use its separate Python environment, load this profile, and build models/tests.
        # The && means dbt runs only if changing to the project folder succeeds.
        bash_command='cd /opt/demo/dbt && /home/airflow/dbt-venv/bin/dbt build --profiles-dir . --target demo',
    # Finish the transformation-task definition.
    )
    # Run loading first; start dbt only after loading succeeds, so reports never use an incomplete batch.
    load >> transform
