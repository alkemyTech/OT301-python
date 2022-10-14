from pathlib import Path
from airflow import DAG

from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from functions.extract import extract
from functions.transform import transform
from functions.load import load

dir = Path(__file__).resolve().parent.parent


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="Universidad_de_buenos_aires",
    default_args=default_args,
    description='ETL DAG for University H data',
    schedule_interval= timedelta(hours=1),
    start_date=datetime(2022, 9, 20)
) as dag:
    t1 = PythonOperator(
        task_id='Get_data',
        python_callable=extract,
        op_kwargs={'file':'GHUNDeBuenosAires'}
    )

    t2 = PythonOperator(
        task_id='transforming_data',
        python_callable=transform,
        op_kwargs={'file':f'GHUNDeBuenosAires_select'}
    )

    t3 = PythonOperator(
        task_id='Uploading_to_s3',
        python_callable=load,
        op_kwargs={
            'file_name':f'{dir}/datasets/GHUNDeBuenosAires_select_process.txt',
            'key': 'Universities_h.txt',
            'bucket_name': 'cohorte-septiembre-5efe33c6',
            'replace': True
            }
    )
    
t1 >> t2 >> t3