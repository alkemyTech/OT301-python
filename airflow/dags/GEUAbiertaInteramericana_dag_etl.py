QUIERO: Configurar un DAG, sin consultas, ni procesamiento
PARA: Hacer un ETL para 2 universidades distintas.
Criterios de aceptación: 
Configurar el DAG para procese la Universidad Abierta Interamericana

from airflow import DAG

from airflow.operators.dummy import DummyOperator

with DAG(
    'GEUAbiertaInteramericana_dag_etl',
    description='Dag etl Grupo E Universidad Abierta Interamericana',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,9,19)
) as dag:

    # Operators for the tasks requested for the DAG
    # They do not call the functions declared above because the DummyOperators do not allow it, but if it is modified later, that connection can be made.

    extraction = DummyOperator(task_id='extraction')
    transformation = DummyOperator(task_id='transformation')
    load = DummyOperator(task_id='load')


    extraction >> transformation >> load