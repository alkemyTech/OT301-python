from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

dag_name = 'GDUNTresDeFebrero_dag_etl'
name_university = 'GDUNTresDeFebrero'

HOME_DIR = Path.home()

# Configuración de Logging
logging.basicConfig(format=f'%(asctime)s - {dag_name} - %(message)s', datefmt='%Y-%m-%d', level=logging.INFO)

# Argumentos del DAG
default_args={
    'owner': 'Alkemy',
    'start_date': datetime(2022, 9, 19),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

logging.info('Iniciando DAG')


POSTGRES_CONN_ID = 'alkemy_db'

def extract_db():
    try:
        with open(f'{HOME_DIR}/OT301-python/airflow/include/{name_university}.sql', 'r') as sqlfile:
            query = sqlfile.read()
        pg_hook = PostgresHook('alkemy_db')
        pg_hook.copy_expert(f"COPY ({query}) TO STDOUT WITH CSV HEADER", filename=f'{HOME_DIR}/OT301-python/airflow/file/{name_university}_select.csv')
        logging.info('Successful extraction')
    except:
        logging.warning('Failure in the extraction process')

def transform_db():
    try:
        df_selec = pd.read_csv(f'{HOME_DIR}/OT301-python/airflow/file/{name_university}_select.csv')
        df_postal_code = pd.read_csv(f'{HOME_DIR}/OT301-python/airflow/assets/codigos_postales.csv')
        logging.info('Successful file reading')
    except:
        logging.warning('Failure to read files')

    try:
        #Limpieza
        df_selec = df_selec.applymap(str)
        for column in df_selec.columns:
            df_selec[column] = df_selec[column].str.strip()
            df_selec[column] = df_selec[column].str.strip('_')
            df_selec[column] = df_selec[column].str.lower()

        df_selec['gender'] = df_selec.gender.replace({'f': 'female', 'm': 'male'})

        #Calculo de la edad
        df_selec['inscription_date'] = pd.to_datetime(df_selec.inscription_date, dayfirst=True)
        df_selec['birth_dates'] = pd.to_datetime(df_selec.birth_dates, dayfirst=True)

        df_selec['age'] = (df_selec['inscription_date'] - df_selec['birth_dates']).astype('<m8[Y]')
        df_selec['age'] = df_selec.age.apply(
            lambda age: age + 100 if (age < 16) else age
        )
        df_selec = df_selec.drop(['birth_dates','location'], axis=1)
        df_selec = df_selec.applymap(str)
        df_selec['inscription_date'] = df_selec['inscription_date'].apply(lambda x: x.split(' ')[0])

        #Asignacion de localidad
        df_postal_code = df_postal_code.applymap(str)
        df_postal_code['localidad'] = df_postal_code['localidad'].str.lower()
        df_postal_code['localidad'] = df_postal_code['localidad'].str.replace(" ", "_")
        df_postal_code = df_postal_code.rename(columns={'codigo_postal':'postal_code','localidad':'location'})

        df_merge = pd.merge(left=df_selec,right=df_postal_code, how='left', left_on='postal_code', right_on='postal_code')
        logging.info('Successful transformation process')
    except:
        logging.warning('Process of transformation failure')

    try:
        df_merge.to_csv(f'{HOME_DIR}/OT301-python/airflow/datasets/{name_university}_process.txt')
        logging.info('Successfully transformed filee')
    except:
        logging.warning('Failure to save transformed file')


with DAG(dag_id=dag_name,
        description='Universidad Nacional Tres de Febrero proceso ETL',
        start_date=datetime(2022,9,19),
        schedule_interval=timedelta(hours=1),
        default_args=default_args,
) as dag:

    # Extraccion de archivos desde base SQL
    extract = PythonOperator(
        task_id='extraccion',
        python_callable=extract_db
    )

    # Transformacion con pandas
    transform = PythonOperator(
        task_id = 'transformacion',
        python_callable=transform_db
    )

    # Carga de datos en S3
    load = DummyOperator(
        task_id = 'carga'
    )

extract >> transform >> load