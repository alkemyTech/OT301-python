from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import logging as log
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
import pandas as pd

# Files
dir = Path(__file__).resolve().parent.parent
sql_path = f'{dir}/include/'
file = 'GHUNDeBuenosAires'

# We configure the registers
log.basicConfig(
    level=log.INFO,
    format='%(asctime)s - %(module)s - %(message)s',
    datefmt='%Y-%m-%d'
)


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

    # Read the sql query, which is in the include folder
    try:
        log.info(f'Reading file {file}.sql')
        with open(f'{sql_path}{file}.sql', 'r') as f:
            query = f.read()
            f.close()
    except Exception as e:
        log.error(f'There was an error reading the query: {e}')

    hook = PostgresHook(postgres_conn_id='alkemy_db')

    # Execute query
    log.info(f'Execute query {file}.sql')
    pandas_df = hook.get_pandas_df(query)

    # Save it as csv
    log.info(f'Saving data in {file}.csv')
    csv_path = f'{dir}/files/{file}_select.csv'
    pandas_df.to_csv(csv_path, sep=',', index=False)

    log.info('Extraction finished')


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
    t1 = EmptyOperator(
        task_id='Get_data'
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

if __name__ == '__main__':
    extract()