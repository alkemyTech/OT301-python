'''COMO:Analista de Datos

OT 301-25
QUIERO: Configurar un DAG, sin consultas, ni procesamiento
PARA: Hacer un ETL para 2 universidades distintas.
Criterios de aceptación: 
Configurar el DAG para procese la Universidad Nacional De La Pampa
Documentar los operators que se deberían utilizar a futuro, teniendo en cuenta que se va a hacer dos consultas SQL (una para cada universidad), se van a procesar los datos con pandas y se van a cargar los datos en S3.  El DAG se debe ejecutar cada 1 hora, todos los días.

OT301-33
QUIERO: Configurar los retries con la conexión al a base de datos
PARA: poder intentar nuevamente si la base de datos me produce un error
Criterios de aceptación: 
Configurar el retry para las tareas del DAG de la Universidad Nacional De La Pampa'''

from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.dummy import DummyOperator


# Declare the dag arguments
default_args = {
    'owner': 'OT301',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':5,
    'retry_delay': timedelta(minutes=5),
}


# Functions to execute when using the DAGS, at this moment they are not called because the DummyOpertors do not allow it

def extraction():
    # Extraction of the required data from the university associated with the database
    pass

def transformation():
    # Processing of data associated with the university
    pass

def load():
    # data load corresponding to the university received as a parameter
    pass


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