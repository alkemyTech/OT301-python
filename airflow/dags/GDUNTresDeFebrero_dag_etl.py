from datetime import datetime
from datetime import timedelta
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

dag_name = 'GDUNTresDeFebrero_dag_etl'
name_university = 'GDUNTresDeFebrero'

# Configuración de Logging
logging.basicConfig(format=f'%(asctime)s - {dag_name} - %(message)s', datefmt='%Y-%m-%d', level=logging.INFO)

# Argumentos del DAG
default_args={
    'owner': 'Alkemy',
    'start_date': datetime(2022, 9, 19),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

logging.info('Iniciando DAG')

POSTGRES_CONN_ID = 'alkemy_db'

def extract_db():
    try:
        with open(f'../OT301-python/airflow/include/{name_university}.sql', 'r') as sqlfile:
            query = sqlfile.read()
        pg_hook = PostgresHook('alkemy_db')
        pg_hook.copy_expert(f"COPY ({query}) TO STDOUT WITH CSV HEADER", filename=f'../OT301-python/airflow/file/{name_university}_select.csv')
        logging.info('Successful extraction')
    except:
        logging.warning('Failure in the extraction process')

def transform_db():
    pass

with DAG(dag_id=dag_name,
        description='Universidad Nacional Tres de Febrero proceso ETL',
        start_date=datetime(2022,9,19),
        schedule_interval=timedelta(hours=1),
        default_args=default_args,
) as dag:

    # Extraccion de archivos desde base SQL
    extract = PythonOperator(
        task_id='extraccion',
        python_callable=extract_db
    )

    # Transformacion con pandas
    transform = DummyOperator(
        task_id = 'transformacion',
        python_callable=transform_db
    )

    # Carga de datos en S3
    load = DummyOperator(
        task_id = 'carga'
    )

extract >> transform >> load