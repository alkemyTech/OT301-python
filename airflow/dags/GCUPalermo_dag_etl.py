
from datetime import timedelta, datetime, date
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from pathlib import Path
import pandas as pd

#Setting information loggers
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(module)s - %(message)s',
    datefmt='%Y-%m-%d')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,                                
    'retry_delay': timedelta(minutes=5)
}

#Setting .sql path
sql_path= Path(__file__).resolve().parents[1]
sql_name= 'GCUPalermo'


#Setting the extraction task
def extraccion():
    with open(f'{sql_path}/include/{sql_name}.sql', 'r') as f:
        query = f.read()
        

    try:
        pg_hook = PostgresHook(postgres_conn_id='alkemy_db')
        logging.info
        (f"-Exporting {sql_name}.")
        df=pg_hook.get_pandas_df(query)
        df.to_csv(f'./OT301-python/airflow/datasets/{sql_name}_select.csv', sep=',')
    except:
        logging.warning
        (f"-Exporting {sql_name} did not perform as expected.")
    
#Transformation task, to set
def transformacion():
    logging.info("Transformando datos")

#Loading task, to set
def cargando():
    logging.info("Guardando datos")   

with DAG(
    'GCUPAlermo_ETL_dag.py',
    default_args= default_args,
    description= 'ETL Universidad Palermo',
    schedule_interval= timedelta (hours=1),     
    start_date= datetime.fromisoformat('2022-09-20'), 
    catchup=False
    ) as dag:

#Tasks execution
    extraccion_task = PythonOperator(task_id='extraccion', python_callable= extraccion)
    transformacion_task = PythonOperator(task_id='transformacion', python_callable= transformacion)
    cargando_task = DummyOperator(task_id='cargando')


    extraccion_task >> transformacion_task >> cargando_task
