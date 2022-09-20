#Dag de ETL para la Universidad Del Salvador

#
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import timedelta,datetime

with DAG(
    'GBUSalvador_dag_etl',
    description = 'ETL para la Universidad Del Salvador del grupo B, OT301-22',
    schedule_interval=timedelta(hours=1),        #Intervalo de ejecución solicitado
    start_date=datetime(2022,9,19)
) as dag:
    extr = DummyOperator(task_id='extr') #Extracción de datos con sentencias sql
    trans = DummyOperator(task_id='trans') #Procesamiento de los datos con Pandas
    load = DummyOperator(task_id='load') #Carga de los datos procesados

    extr >> trans >> load