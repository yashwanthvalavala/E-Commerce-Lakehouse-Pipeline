from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import subprocess
from datetime import timedelta

# Paths **inside container**
BRONZE_SCRIPT = "/opt/airflow/bronze_layer/autoloader.py"
SILVER_SCRIPT = "/opt/airflow/silver_layer/silver transformations.ipynb"
GOLD_SCRIPT = "/opt/airflow/Gold_layer/Gold_transformations.ipynb"

default_args = {
    'owner': 'yashwanth',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Bronze → Silver → Gold ETL pipeline with self-healing placeholder',
    schedule_interval=None,  # triggered manually or via sensors
    start_date=days_ago(1),
    catchup=False,
)

# Function to run Python script
def run_python_script(script_path, **kwargs):
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"{script_path} does not exist!")
    try:
        subprocess.check_call(["python", script_path])
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_path}: {e}")
        raise

# Function to run Jupyter notebook
def run_notebook(script_path, **kwargs):
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"{script_path} does not exist!")
    try:
        subprocess.check_call([
            "jupyter", "nbconvert", "--to", "notebook", "--execute", script_path
        ])
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_path}: {e}")
        raise

# Tasks
bronze_task = PythonOperator(
    task_id='run_bronze_layer',
    python_callable=run_python_script,
    op_kwargs={'script_path': BRONZE_SCRIPT},
    dag=dag
)

silver_task = PythonOperator(
    task_id='run_silver_layer',
    python_callable=run_notebook,
    op_kwargs={'script_path': SILVER_SCRIPT},
    dag=dag
)

gold_task = PythonOperator(
    task_id='run_gold_layer',
    python_callable=run_notebook,
    op_kwargs={'script_path': GOLD_SCRIPT},
    dag=dag
)

# Define pipeline flow
bronze_task >> silver_task >> gold_task
