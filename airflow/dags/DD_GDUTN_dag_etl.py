from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
from pathlib import Path

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

HOME_DIR = Path.home()


# Configuración de Logging
logging.basicConfig(format=f'%(asctime)s - GDUTN_dag_etl - %(message)s', datefmt='%Y-%m-%d', level=logging.INFO)

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
        with open(f'{HOME_DIR}/OT301-python/airflow/include/GDUTN.sql', 'r') as sqlfile:
            query = sqlfile.read()
        pg_hook = PostgresHook('alkemy_db')
        pg_hook.copy_expert(f"COPY ({query}) TO STDOUT WITH CSV HEADER", filename=f'{HOME_DIR}/OT301-python/airflow/file/GDUTN_select.csv')
        logging.info('Successful extraction')
    except:
        logging.warning('Failure in the extraction process')

def transform_db():
    try:
        df_select = pd.read_csv(f'{HOME_DIR}/OT301-python/airflow/file/GDUTN_select.csv')
        df_postal_code = pd.read_csv(f'{HOME_DIR}/OT301-python/airflow/assets/codigos_postales.csv')
        logging.info('Successful file reading')
    except:
        logging.warning('Failure to read files')

    try:
        #Limpieza
        df_select = df_select.applymap(str)
        for column in df_select.columns:
            df_select[column] = df_select[column].str.strip()
            df_select[column] = df_select[column].str.strip('_')
            df_select[column] = df_select[column].str.lower()

            df_select['gender'] = df_select.gender.replace({'f': 'female', 'm': 'male'})
        
        #Calculo de edad
        if "birth_date" in df_select:
            df_select['inscription_date'] = pd.to_datetime(df_select.inscription_date, dayfirst=True)
            df_select['birth_date'] = pd.to_datetime(df_select.birth_date, dayfirst=True)

            df_select['age'] = (df_select['inscription_date'] - df_select['birth_date']).astype('<m8[Y]')
            df_select['age'] = df_select.age.apply(
                lambda age: age + 100 if (age < 16) else age
            )
            df_select = df_select.drop(['birth_date'], axis=1)
            df_select = df_select.applymap(str)
            df_select['inscription_date'] = df_select['inscription_date'].apply(lambda x: x.split(' ')[0])
        else:
            df_select['inscription_date'] = pd.to_datetime(df_select.inscription_date, dayfirst=True)
            df_select['birth_dates'] = pd.to_datetime(df_select.birth_dates, dayfirst=True)

            df_select['age'] = (df_select['inscription_date'] - df_select['birth_dates']).astype('<m8[Y]')
            df_select['age'] = df_select.age.apply(
                lambda age: age + 100 if (age < 16) else age
            )
            df_select = df_select.drop(['birth_dates'], axis=1)
            df_select = df_select.applymap(str)
            df_select['inscription_date'] = df_select['inscription_date'].apply(lambda x: x.split(' ')[0])

        #Asignacion de codigo postal / localidad
        df_postal_code = df_postal_code.applymap(str)
        df_postal_code['localidad'] = df_postal_code['localidad'].str.lower()
        df_postal_code['localidad'] = df_postal_code['localidad'].str.replace(" ", "_")
        df_postal_code = df_postal_code.rename(columns={'codigo_postal':'postal_code','localidad':'location'})

        if str(df_select['postal_code'].loc[0]) == 'nan':
            df_select = df_select.drop(['postal_code'], axis=1)
            df_select['location'] = df_select['location'].str.replace(" ", "_")
            df_merge = pd.merge(left=df_select,right=df_postal_code, how='left', left_on='location', right_on='location')

        elif str(df_select['location'].loc[0]) == 'nan':
            df_select = df_select.drop(['location'], axis=1)
            df_merge = pd.merge(left=df_select,right=df_postal_code, how='left', left_on='postal_code', right_on='postal_code')
        
        logging.info('Successful transformation process')
    except:
        logging.warning('Process of transformation failure')
    
    try:
        df_merge.to_csv(f'{HOME_DIR}/OT301-python/airflow/datasets/GDUTN_process.txt')
        logging.info('Successfully transformed filee')
    except:
        logging.warning('Failure to save transformed file')
    
def upload_to_s3(filename:str,key:str,bucket_name:str) -> None:
    try:
        hook = S3Hook('aws_s3_bucket')
        hook.load_file(filename=filename,key=key,bucket_name=bucket_name,replace=True)
        logging.info('Successful upload to S3')
    except:
        logging.warning('failure in the process of loading to S3')


with DAG(dag_id='DD_GDUTN_dag_etl',
        description='Universidad Tecnolgogica Nacional proceso ETL',
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
    load = PythonOperator(
        task_id = 'carga',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': f'{HOME_DIR}/OT301-python/airflow/datasets/GDUTN_process.txt',
            'key': f'GDUTN_process.txt',
            'bucket_name': 'cohorte-septiembre-5efe33c6'
        }
    )

extract >> transform >> load