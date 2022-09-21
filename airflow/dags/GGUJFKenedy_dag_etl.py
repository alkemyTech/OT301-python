from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
import logging

# create logger
logger = logging.getLogger("GGUJFKenedy_dag_etl")
logger.setLevel(logging.DEBUG)

# console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# create formatter
log_formatter = logging.Formatter("%(asctime)s - %(name)s - %(message)s", "%Y-%m-%d")

# add formatter to cons_hand
console_handler.setFormatter(log_formatter)

# add cons_handler to logger
logger.addHandler(console_handler)

# messages examples
logger.debug("Dag_debugg")
logger.info("Dag running normally")
logger.warning("Warn message")
logger.error("Problems running ETL")
logger.critical("Had to break! Huge problems.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'GGFLatinoamericanaCsSociales_dags',
    description='Dag for Facultad Latinoamericana de Ciencias Sociales',
    schedule_interval='%@hourly',
    start_date=datetime(2022, 1, 1), # Random start_date, not asked in the tasks but not able to skip this argument as well.
) as dag:
    task_1=DummyOperator(task_id='GGUJFKenedy_getting_data_from_database')
    task_2=DummyOperator(task_id='GGUJFKenedy_proc_with_pandas')
    task_3=DummyOperator(task_id='GGUJFKenedy_load_data_in_S3')

# Using 'except logging.exception:' IS NOT a good practice in Python, specially for Debugg, but until we get to the part where we know 
# exactly what DAGs are going to do, which tools we are going to need to make them work and what exceptions/errors we will have to deal 
# with, or at least expect, this should work as a provisional 'tool' in order to complete the first scrum.

try:
    task_1
    logger.info("Connection to database succesful!")
except logging.exception:
    logger.error("Problems connecting to database!")

try:
    task_2
    logger.info("Data transforming running normally")
except logging.exception:
    logger.error("Problems transforming data!")

try:
    task_3
    logger.info("Uploading data running normally")
except logging.exception:
    logger.error("Problems uploading data to S3!")