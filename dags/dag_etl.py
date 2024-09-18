from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

import os

from modules import create_sql_objects, run_etl

# DAG
default_args={
    'owner': 'FerSoriano',
    'depends_on_past': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1), # 1 min de espera antes de cualquier re intento
    'email': [os.getenv('AIRFLOW_VAR_EMAIL')],
    'email_on_retry': True
}


with DAG(
    dag_id="etl_process",
    default_args= default_args,
    description="ETL process to get Olympic Games data",
    start_date=datetime(2024,8,30),
    schedule='@daily',
    tags=['DE','proyecto_final'],
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

    # 4 - Email Notification
    t_email_notification_success = EmailOperator(
        task_id='email_notification',
        to=os.getenv('AIRFLOW_VAR_TO_ADDRESS'),
        subject=os.getenv('AIRFLOW_VAR_SUBJECT_MAIL'),
        html_content="El DAG en Airflow, {{ dag.dag_id }}, finalizó correctamente."
    )

    # Email on failure
    t_email_notification_failure = EmailOperator(
        task_id='email_notification_failure',
        to=os.getenv('AIRFLOW_VAR_TO_ADDRESS'),
        subject='Fallo en el DAG {{ dag.dag_id }}',
        html_content="El DAG {{ dag.dag_id }} ha fallado.",
        trigger_rule='one_failed'  # Se ejecuta si alguna tarea falla
    )

    # Task dependencies
    t_create_sql_objects >> t_run_etl >> t_completed_message >> t_email_notification_success
    [t_create_sql_objects, t_run_etl, t_completed_message] >> t_email_notification_failure