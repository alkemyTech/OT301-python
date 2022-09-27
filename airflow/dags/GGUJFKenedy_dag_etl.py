from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import logging
import pandas as pd
import os

# create logger
logger = logging.getLogger("GGUJFKenedy_logging")
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

# get datasets directory
datasets_path=airflow_path+'/datasets/'
print(datasets_path)

try:
  reading_query= open(include_path+'GGUJFKenedy.sql','r')
  sql_query = reading_query.read()
  reading_query.close
except FileNotFoundError:
  logging.error('Could not find .sql file.')

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
  logging.info('Getting PostgresHook on Kennedy')
  df=pg_hook.get_pandas_df(sql=sql_query)
  csv_file=df.to_csv(datasets_path+'GGUJFKenedy_select.csv',sep=',', index=False) # It works despise to_csv function
  logging.info('GGUJFKenedy_select.csv file created!')                            # not being recognized
  return csv_file

with DAG(
  dag_id='GGUJFKenedy_dag',
  description='Dag para la Universidad JFKennedy',
  schedule=timedelta(hours=1),
  start_date=datetime(2022, 1, 1),
  default_args=default_args,
  template_searchpath = include_path
) as dag:

  extraction=PythonOperator(task_id='kennedy_extract', python_callable=get_data_from_db)
  transformation_task=EmptyOperator(task_id='kennedy_transormation')

  extraction >> transformation_task

