'''COMO: Analista de Datos

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
Configurar el retry para las tareas del DAG de la Universidad Nacional De La Pampa

OT301-41
QUIERO: Configurar los log 
PARA: Mostrarlos en consola
Criterios de aceptación:
- Configurar logs para Universidad Nacional De La Pampa
- Use la librería de Loggin de python: https://docs.python.org/3/howto/logging.html
- Realizar un log al empezar cada DAG con el nombre del logger
- Formato del log: %Y-%m-%d - nombre_logger - mensaje 
Aclaración: 
Deben dejar la lista de configuración para que se pueda incluir dentro de las funciones futuras. No es necesario empezar a escribir registros.

OT301-49
QUIERO: Implementar SQL Operator
PARA: tomar los datos de las bases de datos en el DAG
Criterios de aceptación: Configurar un Python Operators, para que extraiga información de la base de datos utilizando el .sql disponible en el repositorio base de la Universidad Nacional De La Pampa
Dejar la información en un archivo .csv dentro de la carpeta datasets.

OT301-57
QUIERO: Implementar el Python Operator
PARA: procesar los datos obtenidos de la base de datos dentro del DAG
Criterios de aceptación: 
Configurar el Python Operator para que ejecute la función que procese los datos para la Universidad Nacional de La Pampa'''

from datetime import timedelta, datetime, date

from airflow import DAG

from airflow.operators.dummy import DummyOperator

from airflow.operators.python import PythonOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd

import logging

from pathlib import Path

from os import remove


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
    airflow_folder = Path(__file__).resolve().parent.parent
    university = 'GEUNDeLaPampa'
    try:
        # Reading the query for this particular university
        file_sql = open(f'{airflow_folder}/include/{university}.sql','r')
        query = file_sql.read()
        file_sql.close()

        # connecting to the database
        hook = PostgresHook(postgres_conn_id='alkemy_db')

        # execution of the query to save in a pandas dataframe
        df = hook.get_pandas_df(query)

        # If it exists, I delete the file generated previously to update the information.
        try:
            remove(f'{airflow_folder}/files/{university}_select.csv')
        except:
            pass

        # export to a .csv file in the folder suggested in the issue
        df.to_csv(f'{airflow_folder}/files/{university}_select.csv',sep=';')

        logging.info(f"{date.today().year}-{date.today().month}-{date.today().day} - Start SQL - extraction done successfully")
        
    except:
        logging.warning(f"{date.today().year}-{date.today().month}-{date.today().day} - Start SQL - extraction was not performed correctly")


def transformation():
    # Processing of data associated with the university
    try:
        # implementation of the function
        logging.info(f"{date.today().year}-{date.today().month}-{date.today().day} - Start SQL - transformation done successfully")
    except:
        logging.warning(f"{date.today().year}-{date.today().month}-{date.today().day} - nombre_logger - transformation was not performed correctly")


def load():
    # data load corresponding to the university received as a parameter
    try:
        # implementation of the function
        logging.info(f"{date.today().year}-{date.today().month}-{date.today().day} - Start SQL - load done successfully")
    except:
        logging.warning(f"{date.today().year}-{date.today().month}-{date.today().day} - Start SQL - load was not performed correctly")


with DAG(
    'GEUNDeLaPampa_dag_etl',
    description='Dag etl Grupo E Universidad Nacional de La Pampa',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,9,19)
) as dag:

    # Operators for the tasks requested for the DAG

    extraction = PythonOperator(
        task_id='extraction',
        dag=dag,
        python_callable=extraction
    )
    
    transformation = PythonOperator(
        task_id='transformation',
        dag=dag,
        python_callable=transformation
    )
    
    load = DummyOperator(task_id='load')


    extraction >> transformation >> load