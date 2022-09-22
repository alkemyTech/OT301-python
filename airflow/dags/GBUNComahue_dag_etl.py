#Importamos las librerias que utilizaremos

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import timedelta,datetime
import logging

# Le damos la configuración base a logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s', 
                    datefmt='%Y/%m/%d')
logging.info('Dags iniciado')               #Creamos el registro de que se inicio el dag

default_args = {
    'retries': 5,                           #Configuramos los retries
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'GBUNComahue_dag_etl',
    description = 'ETL para la Universidad Nacional Del Comahue del grupo B',
    schedule_interval=timedelta(hours=1),   #Intervalo de ejecución solicitado
    start_date=datetime(2022,9,19),
    default_args=default_args
) as dag:
    extr = DummyOperator(task_id='extr')    #Extracción de datos con sentencias sql
    trans = DummyOperator(task_id='trans')  #Procesamiento de los datos con Pandas
    load = DummyOperator(task_id='load')    #Carga de los datos procesados

    extr >> trans >> load