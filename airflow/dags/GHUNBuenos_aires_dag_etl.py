from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging as log

# We configure the registers
log.basicConfig(
    level=log.INFO,
    format='%(asctime)s - %(module)s - %(message)s',
    datefmt='%Y-%m-%d'
)
logger = log.getLogger('Starting the DAG')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def extract():
    """
    Function that is responsible for extracting the data, from the include folder of the group h
    """
    print('Extraction')


def transform():
    """
    Function that is responsible for transforming the data
    """
    print('Clean data')


def load():
    """
    Function that is responsible for uploading the data to amazon s3
    """
    print('Uploaded to s3')


with DAG(
        'Buenos_Aires_University',  # Dagger name
        default_args=default_args,  # This will automatically apply it to any operators bound to it
        description='ETL DAG for University H data',  # Dags description
        start_date=datetime(2022, 9, 20),  # Dag boot date
        schedule_interval=timedelta(hours=1),  # The dag is going to run every 1 hour
        catchup=False

) as dag:
    t1 = PythonOperator(
        task_id='Get_data',
        dag=dag,
        python_callable=extract

    )

    t2 = PythonOperator(
        task_id='transforming_data',
        dag=dag,
        python_callable=transform
    )

    t3 = PythonOperator(
        task_id='Uploading_to_s3',
        dag=dag,
        python_callable=load
    )

t1 >> t2 >> t3