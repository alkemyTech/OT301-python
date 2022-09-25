#Dag de ETL para la Universidad Del Salvador

#Importamos las librerias que utilizaremos
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import timedelta,datetime
from pandas import DataFrame
import logging
from os import path

# Le damos la configuración base a logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s', 
                    datefmt='%Y/%m/%d')
logging.info('Dags iniciado')               #Creamos el registro de que se inicio el dag

default_args = {
    'retries': 5,                           #Configuramos los retries
    'retry_delay': timedelta(minutes=5)
}
path_dags = path.dirname(path.realpath(__file__)) #Guardo la dirección de este .py
name_un = 'GBUSalvador'                           #Nombre de la universidad

#Defino la función que conectara a la bd y creará el archivo .csv
def extract():                              
    #Leo el archivo .sql y guardo la sentencia en una variable
    with open(path_dags.replace('/dags',f'/include/{name_un}.sql'),'r') as f:
        query = f.read()                    
        f.close()
    #Realizo la extración
    try:
        pg_hook = PostgresHook(postgres_conn_id='alkemy_db')
        df_data = pg_hook.get_pandas_df(sql=query)
        logging.info('Exporting query to file')
        df_data.to_csv(path_dags.replace('/dags',f'/datasets/{name_un}_select.csv'))
        logging.info('Exporting finished')
    except:
        logging.warning('Data base - Connection failed')

with DAG(
    'GBUSalvador_dag_etl',
    description = 'ETL para la Universidad Del Salvador',
    schedule_interval=timedelta(hours=1),   
    start_date=datetime(2022,9,19),
    default_args=default_args,
    template_searchpath=path_dags.replace('/dags','/include')
) as dag:
    extract_task = PythonOperator(task_id='extr',python_callable=extract) #Extracción de datos
    trans = DummyOperator(task_id='trans')  #Procesamiento de los datos con Pandas
    load = DummyOperator(task_id='load')    #Carga de los datos procesados

    extract_task >> trans >> load