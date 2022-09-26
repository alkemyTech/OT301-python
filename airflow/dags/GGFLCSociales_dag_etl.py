from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import logging
import pandas as pd
import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# create logger
logger = logging.getLogger("GGFLCSociales_logging")
logger.setLevel(logging.DEBUG)

# console handler
cons_handler = logging.StreamHandler()
cons_handler.setLevel(logging.INFO)

# create formatter
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d")

# add formatter to cons_hand
cons_handler.setFormatter(log_formatter)

# add cons_handler to logger
logger.addHandler(cons_handler)


# get current directory (dags)
dags_path = os.path.dirname(os.path.realpath(__file__))
print('dags_path',dags_path)

# get airflow directory
airflow_path=os.path.abspath(os.path.join(dags_path, os.pardir))
print('airflow_path',os.path.abspath(os.path.join(airflow_path, os.pardir)))

# get include directory
include_path=airflow_path+'/include/'
print('include_directory',include_path)

# get files directory
files_path=airflow_path+'/files/'
print(files_path)

# get datasets directory
datasets_path=airflow_path+'/datasets/'
print(datasets_path)

try:
  reading_query= open(include_path+'GGFLCSociales.sql','r')
  sql_query = reading_query.read()
  reading_query.close
except FileNotFoundError:
  logging.error('Could not find GGFLCSociales.sql file.')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

def get_data_from_db():
  pg_hook=PostgresHook(postgres_conn_id='alkemy_db', schema='training')
  logging.info('Getting PostgresHook on Sociales')
  df=pg_hook.get_pandas_df(sql=sql_query)
  csv_file=df.to_csv(files_path+'GGFLCSociales_select.csv',sep=',', index=False) # It works despise to_csv function
  logging.info('GGFLCSociales_select.csv file created!')                            # not being recognized
  return csv_file

# Convention: If duplicate data is found, keep the first one. So, this will remove duplicated values beyond the first.
try:
  codigos_postales_file_path=airflow_path+'/assets/codigos_postales.csv'
  cp_dataframe=pd.read_csv(codigos_postales_file_path)
  cp_dataframe['localidad']=cp_dataframe['localidad'].str.lower()
  cp_dataframe.rename({'codigo_postal':'postal_code'}, axis=1, inplace=True)
  cp_dataframe.rename({'localidad':'location'}, axis=1, inplace=True)
  cp_dataframe=cp_dataframe.drop_duplicates(subset='location')
  cp_dataframe=cp_dataframe[['location','postal_code']]
except FileNotFoundError:
  logging.warning('Could not find codigos_postales.csv file!')
except KeyError:
  logging.warning('Check the names of the columns (Function vs codigos_postales.csv file).')


def data_transformation():
  pg_hook=PostgresHook(postgres_conn_id='alkemy_db', schema='training')
  logging.info('Getting PostgresHook on Sociales')
  df=pg_hook.get_pandas_df(sql=sql_query)
  # Setting config to change data format as requested. ¡first_name and last_name would remain the same due to a convention!
  df['university']=df['university'].str.lower().str[1:].str.replace('-',' ')
  df['career']=df['career'].str.lower().str.replace('-',' ')
  df['inscription_date']=pd.to_datetime(df['inscription_date']).dt.strftime('%Y-%m-%d').astype(str)
  df['gender']=df['gender'].replace(['M','F'],['male','female'])
  df['location']=df['location'].str.lower().str.replace('-',' ')
  df['email']=df['email'].str.lower().str.replace('-',' ')
  df['age']=df['age'].astype(int)
  df.drop('postal_code', axis=1, inplace=True)
  df=df.merge(cp_dataframe,on='location',how='left')
  df=df[['university','career','inscription_date','first_name','last_name','gender','age','location','postal_code','email']]
  processed_csv_file=df.to_csv(datasets_path+'GGFLCSociales_select.csv',sep=',', index=False)
  return processed_csv_file

with DAG(
  dag_id='GGFLCSociales_dag',
  description='Dag para la Facultad Latinoamericana de Ciencias Sociales',
  schedule=timedelta(hours=1),
  start_date=datetime(2022, 1, 1),
  default_args=default_args,
  template_searchpath = include_path
) as dag:

  extraction_task=PythonOperator(task_id='sociales_extract', python_callable=get_data_from_db)
  transformation_task=PythonOperator(task_id='sociales_transormation', python_callable=data_transformation)

  extraction_task >> transformation_task