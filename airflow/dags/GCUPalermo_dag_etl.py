from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
#Se utilizara PythonOperator para ejecutar las funciones de extraccion, transformacion y carga. 
#from airflow.operators.python import PythonOperator
import logging 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['ferduarte@live.com.ar'], 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,                                #Configuracion de retries
    'retry_delay': timedelta(minutes=5)
}

#Logs con mensajes de informacion sobre la tarea que se esta ejecutando
def extraccion():
    logging.info("Extrayendo datos")
def transformacion():
    logging.info("Transformando datos")
def cargando():
    logging.info("Guardando datos")   
    
with DAG(
    'GCUPalermo_dag_etl.py',
    default_args= default_args,
    description= 'ETL Universidad Palermo',
    schedule_interval= timedelta (hours=1),       #Configuracion intervalo de ejecucion
    start_date= datetime.fromisoformat('2022-09-20'), 
    catchup=False
    ) as dag:
    
#Ejecucion de tareas
    extraccion_task = DummyOperator(task_id='extraccion')
    transformacion_task = DummyOperator(task_id='transformacion')
    cargando_task = DummyOperator(task_id='cargando')
    
    
    extraccion_task >> transformacion_task >> cargando_task
    
