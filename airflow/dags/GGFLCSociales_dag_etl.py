from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'GGFLatinoamericanaCsSociales_dags',
    description='Dag para la Facultad Latinoamericana de Ciencias Sociales',
    schedule_interval='%@hourly',
    start_date=datetime(2022, 1, 1),
) as dag:
    task_1=DummyOperator(task_id='GGFLatinoamericanaCsSociales')
    task_2=DummyOperator(task_id='Proc_with_Pandas')
    task_3=DummyOperator(task_id='Load_data_in_S3')

task_1 >> task_2 >> task_3