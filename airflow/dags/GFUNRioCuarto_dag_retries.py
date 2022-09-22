''' Configura los retries con la conexión a la base de datos
para poder intentar nuevamente si la base de datos me produce un error.
Configurar el retry para las tareas del DAG de la Universidad Nacional De Morón '''

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
    'GFUNRioCuarto_dag_retries',
    description='Dag para la Universidad Nacional de Rio Cuarto',
    schedule_interval='%@hourly',
    start_date=datetime(2022, 1, 20),
) as dag:
    tarea_1=DummyOperator(task_id='GFUNRioCuarto')
    tarea_2=DummyOperator(task_id='Proc_with_Pandas')
    tarea_3=DummyOperator(task_id='Load_data_in_S3')

tarea_1 >> tarea_2 >> tarea_3