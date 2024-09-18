from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from modules import create_sql_objects, run_etl

# DAG
default_args={
    'owner': 'FerSoriano',
    'depends_on_past': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=2) # 2 min de espera antes de cualquier re intento
}


with DAG(
    dag_id="etl_process",
    default_args= default_args,
    description="ETL process to get Olympic Games data",
    start_date=datetime(2024,8,30),
    schedule='@daily',
    tags=['DE','Preentrega3'],
    catchup=False
    ) as dag:

    # 1 - Create Tables and Views
    t_create_sql_objects = PythonOperator(
        task_id='create_sql_objects',
        python_callable=create_sql_objects
    )

    # 2 - Run ETL process
    t_run_etl = PythonOperator(
        task_id='etl_process',
        python_callable=run_etl
    )

    # 3 - Completed message
    t_completed_message = BashOperator(
        task_id= 'completed_message',
        bash_command='echo Process completed'
    )

    # Task dependencies
    t_create_sql_objects >> t_run_etl >> t_completed_message