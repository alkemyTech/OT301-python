from datetime import timedelta
import logging as log
from airflow import DAG

# We configure the registers
log.basicConfig(
    level=log.INFO,
    format='%(asctime)s - %(module)s - %(message)s',
    datefmt='%Y-%m-%d'
)

logger = log.getLogger('Starting the DAG of group h')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'University_H',
    default_args=default_args,
    description='ETL DAG for University H data',
    
) as dag: