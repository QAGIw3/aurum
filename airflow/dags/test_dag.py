"""
Simple test DAG for Airflow in Kind cluster.
This DAG demonstrates basic functionality and can be used to verify the setup.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'aurum',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_aurum_dag',
    default_args=default_args,
    description='A simple test DAG for Aurum Airflow setup',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['test', 'aurum'],
)

def print_hello():
    """Simple Python function to test Airflow."""
    print("Hello from Aurum Airflow!")
    return "Hello World from Aurum!"

# Python task
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

# Bash task
bash_task = BashOperator(
    task_id='bash_task',
    bash_command='echo "Bash task executed successfully!"',
    dag=dag,
)

# Set task dependencies
hello_task >> bash_task
