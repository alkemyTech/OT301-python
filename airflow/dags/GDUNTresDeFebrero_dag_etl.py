from datetime import datetime
from datetime import timedelta
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

dag_name = 'UNTresDeFebrero_dag_etl'

# Configuración de Logging
logging.basicConfig(format=f'%(asctime)s - {dag_name} - %(message)s', datefmt='%Y-%m-%d', level=logging.INFO)

# Configuración del DAG
default_args={
    'owner': 'Alkemy',
    'start_date': datetime(2022, 9, 19),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

logging.info('Iniciando DAG')

with DAG(dag_id=dag_name,
        description='Universidad Nacional Tres de Febrero proceso ETL',
        start_date=datetime(2022,9,19),
        schedule_interval=timedelta(hours=1),
        default_args=default_args,
) as dag:

    # Extraccion de archivos desde base SQL
    extract = DummyOperator(
        task_id='extraccion'
    )

    # Transformacion con pandas
    transform = DummyOperator(
        task_id = 'transformacion'
    )

    # Carga de datos en S3
    load = DummyOperator(
        task_id = 'carga'
    )

extract >> transform >> load