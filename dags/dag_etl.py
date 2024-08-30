from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules import connect_database, create_sql_objects, run_etl, close_connection

# DAG
default_args={
    'owner': 'FerSoriano',
    'retries': 5,
    'retry_delay': timedelta(minutes=2) # 2 min de espera antes de cualquier re intento
}


with DAG(
    dag_id="etl_process",
    default_args= default_args,
    description="ETL process to get Olympic Games data",
    start_date=datetime(2024,8,29),
    schedule='@daily',
    tags=['DE','Preentrega3'],
    catchup=False
    ) as dag:

    # 1 - Connect to the Database
    t_connect_database = PythonOperator(
        task_id='database_connection',
        python_callable=connect_database
    )

    # 2 - Create Tables and Views
    t_create_sql_objects = PythonOperator(
        task_id='create_sql_objects',
        python_callable=create_sql_objects
    )

    # 3 - Run ETL process
    t_run_etl = PythonOperator(
        task_id='etl_process',
        python_callable=run_etl
    )

    # 4 - Close connection
    t_close_connection = PythonOperator(
        task_id='close_connection',
        python_callable=close_connection
    )

    # Task dependencies
    t_connect_database >> t_create_sql_objects >> t_run_etl >> t_close_connection