'''COMO: Analista de Datos
Sprint 1: OT301-25 OT301-33 OT301-41

OT301-49
QUIERO: Implementar SQL Operator
PARA: tomar los datos de las bases de datos en el DAG
Criterios de aceptación: Configurar un Python Operators, para que extraiga información de la base de datos utilizando el .sql disponible en el repositorio base de la Universidad Nacional De La Pampa
Dejar la información en un archivo .csv dentro de la carpeta datasets.

OT301-57
QUIERO: Implementar el Python Operator
PARA: procesar los datos obtenidos de la base de datos dentro del DAG
Criterios de aceptación: 
Configurar el Python Operator para que ejecute la función que procese los datos para la Universidad Nacional de La Pampa

OT301-65
QUIERO: Crear una función Python con Pandas para cada universidad
PARA: poder normalizar los datos de las mismas
Criterios de aceptación: 
Una funcion que devuelva un txt para cada una de launiversidad Abierta Interamericana
Datos Finales:
- university: str minúsculas, sin espacios extras, ni guiones
- career: str minúsculas, sin espacios extras, ni guiones
- inscription_date: str %Y-%m-%d format
- first_name: str minúscula y sin espacios, ni guiones
- last_name: str minúscula y sin espacios, ni guiones
- gender: str choice(male, female)
- age: int
- postal_code: str
- location: str minúscula sin espacios extras, ni guiones
- email: str minúsculas, sin espacios extras, ni guiones
Aclaraciones:
Para calcular codigo postal o locación se va a utilizar el .csv que se encuentra en el repo.
La edad se debe calcular en todos los casos

OT301-77
QUIERO: Utilizar un operador creado por la comunidad
PARA: poder subir el txt creado por el operador de Python al S3
Criterios de aceptación: 
- Tomar el .txt del repositorio base 
- Buscar un operador creado por la comunidad que se adecue a los datos.
- Configurar el S3 Operator para la Universidad Nacional De La Pampa
- Subir el archivo a S3'''

from datetime import timedelta, datetime, date

from airflow import DAG

from airflow.operators.dummy import DummyOperator

from airflow.operators.python import PythonOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.hooks.S3_hook import S3Hook

import pandas as pd

import logging

from os import remove

from pathlib import Path

from boto3.exceptions import S3UploadFailedError

from botocore.exceptions import ClientError

# declare global variables
airflow_folder = Path(__file__).resolve().parent.parent
university = 'GEUNDeLaPampa'


# Declare the dag arguments
default_args = {
    'owner': 'OT301',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':5,
    'retry_delay': timedelta(minutes=5),
}


# Functions to execute when using the DAGS, at this moment they are not called because the DummyOpertors do not allow it

# Extraction of the required data from the university associated with the database
def extraction():
    try:
        # Reading the query for this particular university
        file_sql = open(f'{airflow_folder}/include/{university}.sql','r')
        query = file_sql.read()
        file_sql.close()

        # connecting to the database
        hook = PostgresHook(postgres_conn_id='alkemy_db')

        # execution of the query to save in a pandas dataframe
        df = hook.get_pandas_df(query)

        # If it exists, I delete the file generated previously to update the information.
        try:
            remove(f'{airflow_folder}/files/{university}_select.csv')
        except:
            pass

        # export to a .csv file in the folder suggested in the issue
        df.to_csv(f'{airflow_folder}/files/{university}_select.csv',sep=';')

        logging.info(f"{date.today().year}-{date.today().month}-{date.today().day} - Start SQL - extraction done successfully")
        
    except:
        logging.warning(f"{date.today().year}-{date.today().month}-{date.today().day} - Start SQL - extraction was not performed correctly")


# Processing of data associated with the university
def transformation():
    try:
        # reading the csv file extracted from the database
        df = pd.read_csv(f'{airflow_folder}/files/{university}_select.csv', sep=';')

        # college enrollment age is calculated
        df['age'] = pd.to_datetime(df['age'],dayfirst=True)
        df['inscription_date'] = pd.to_datetime(df['inscription_date'], dayfirst=True)
        df['age'] = df['inscription_date']-df['age']
        df['age'] = (df['age'].dt.days/365.25).astype(int)

        # the parameters university, carrer, inscription_date, gender and email are accommodated as requested
        df['university'] = df['university'].str.lower().str.rstrip().str.lstrip()
        df['career'] = df['career'].str.lower().str.rstrip().str.lstrip()
        df['inscription_date'] = df['inscription_date'].astype(str)
        df['gender'] = df['gender'].replace({'F':'female','M':'male'})
        df['email'] = df['email'].str.lower().str.rstrip().str.lstrip()

        # the name is separated and the format is accommodated
        name = df['last_name'].str.lower().str.rstrip().str.lstrip().str.split(' ').to_list()
        columns = ['first_name','last_name','-','-']
        name = pd.DataFrame(name, columns=columns)
        df['first_name'] = name['first_name']
        df['last_name'] = name['last_name']

        # the missing location parameter is filled according to the "postal codes" file located in the assets
        file = f'{airflow_folder}/assets/codigos_postales.csv'
        df_cp = pd.read_csv(file, sep=',')
        df_cp.rename({'codigo_postal':'postal_code'}, axis=1, inplace=True)
        df_cp.rename({'localidad':'location'}, axis=1, inplace=True)
        df_cp.drop_duplicates(subset='postal_code')
        df.drop('location', axis=1, inplace=True)
        df = df.merge(df_cp, on='postal_code', how='left')
        df['location'] = df['location'].str.lower().str.rstrip().str.lstrip()

        # leave the columns that interest me for the file
        df=df[['university','career','inscription_date','first_name','last_name','gender','age','postal_code','location','email']]

        # If it exists, I delete the file generated previously to update the information.
        try:
            remove(f'{airflow_folder}/datasets/{university}_process.txt')
        except:
            pass

        # export to a .txt file in the folder suggested in the issue
        df.to_csv(f'{airflow_folder}/datasets/{university}_process.txt', sep=';')

        logging.info(f"{date.today().year}-{date.today().month}-{date.today().day} - Process - transformation done successfully")
    except:
        logging.warning(f"{date.today().year}-{date.today().month}-{date.today().day} - Process - transformation was not performed correctly")


# data load corresponding to the university received as a parameter
def load(filename: str, key: str, bucket_name: str) -> None:
    try:
        hook = S3Hook(aws_conn_id='aws_s3_bucket')
        hook.load_file(filename=filename, key=key,bucket_name=bucket_name)
        logging.info(f"{date.today().year}-{date.today().month}-{date.today().day} - Load - load done successfully")
    except ValueError:
        logging.warning('File could already exist in s3 bucket destination. Check it.')
    except FileNotFoundError:
        logging.warning('Could not find the file')
    except S3UploadFailedError:
        logging.error('Bucket destination does not exist. Please check its name either on aws or in the code.')
    except ClientError:
        logging.error('Error connecting to Bucket. Check for Admin->Connections->aws_s3_bucket Key and SecretKey loaded data.')
    except:
        logging.warning(f"{date.today().year}-{date.today().month}-{date.today().day} - Load - load was not performed correctly")


with DAG(
    'GEUNDeLaPampa_dag_etl',
    description='Dag etl Grupo E Universidad Nacional de La Pampa',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,9,19)
) as dag:

    # Operators for the tasks requested for the DAG

    extraction = PythonOperator(
        task_id='extraction',
        dag=dag,
        python_callable=extraction
    )
    
    transformation = PythonOperator(
        task_id='transformation',
        dag=dag,
        python_callable=transformation
    )
    
    load = PythonOperator(
        task_id='load',
        dag=dag,
        python_callable=load,
        op_kwargs={
            'filename': f'{airflow_folder}/datasets/{university}_process.txt',
            'key': f'{university}_process.txt',
            'bucket_name': 'cohorte-septiembre-5efe33c6',
        }
    )


    extraction >> transformation >> load