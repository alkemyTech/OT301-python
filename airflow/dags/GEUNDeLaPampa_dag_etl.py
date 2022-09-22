QUIERO: Configurar un DAG, sin consultas, ni procesamiento
PARA: Hacer un ETL para 2 universidades distintas.
Criterios de aceptación: 
Configurar el DAG para procese la Universidad Nacional De La Pampa
from airflow import DAG

from airflow.operators.dummy import DummyOperator

# Functions to execute when using the DAGS, at this moment they are not called because the DummyOpertors do not allow it

def extraction():
    # Extraction of the required data from the university associated with the database

with DAG(
    'GEUNDeLaPampa_dag_etl',
    description='Dag etl Grupo E Universidad Nacional de La Pampa',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,9,19)
) as dag:

    # Operators for the tasks requested for the DAG
    # They do not call the functions declared above because the DummyOperators do not allow it, but if it is modified later, that connection can be made.

    extraction = DummyOperator(task_id='extraction')
    transformation = DummyOperator(task_id='transformation')
    load = DummyOperator(task_id='load')


    extraction >> transformation >> load