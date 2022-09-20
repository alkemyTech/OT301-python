#Dag de ETL para la Universidad Del Salvador

#Librerias que necesitaremos
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import timedelta,datetime

default_args = {
    'retries': 5,               #Configuro los retries
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'GBUSalvador_dag_etl',
    description = 'ETL para la Universidad Del Salvador del grupo B',
    schedule_interval=timedelta(hours=1),        #Intervalo de ejecución
    start_date=datetime(2022,9,19),
    default_args=default_args
) as dag:
    extr = DummyOperator(task_id='extr') #Extracción de datos con sentencias sql
    trans = DummyOperator(task_id='trans') #Procesamiento de los datos con Pandas
    load = DummyOperator(task_id='load') #Carga de los datos procesados

    extr >> trans >> load