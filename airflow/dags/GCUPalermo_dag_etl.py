from datetime import timedelta, datetime
from airflow import DAG

#Se utilizara PythonOperator para ejecutar las funciones de extraccion, transformacion y carga. 
from airflow.operators.python import PythonOperator
import logging 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['ferduarte@live.com.ar'], 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': '',                                #Configuracion de retries
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
    start_date= datetime(2022-9-20), 
    catchup=False,
    template_searchpath='''direccion de extraccion sqls''', 
    ) as dag:
    
    #Ejecucion de tareas
    extraccion_task = ""
    transformacion_task = ""
    cargando_task = ""
    
    
    extraccion_task >> transformacion_task >> cargando_task
    