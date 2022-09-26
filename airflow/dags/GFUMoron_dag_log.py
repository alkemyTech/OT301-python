''' Configurar los log para mostrarlos en consola para la Universidad de Moron '''


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
import logging

#create logger
logger = logging.getLogger("dags_logging")
logger.setLevel(logging.DEBUG)

#console handler
cons_handler = logging.StreamHandler()
cons_handler.setLevel(logging.INFO)

#create formatter
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d")

#add formatter to cons_hand
cons_handler.setFormatter(log_formatter)

#add cons_handler to logger
logger.addHandler(cons_handler)

#messages examples
logger.debug("Dag_debugg")
logger.info("Dag running normally")
logger.warning("Warn message")
logger.error("Problems running ETL")
logger.critical("Had to break! Problems.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'GFUMoron_dag_log',
    description='Dag para la Universidad de Moron',
    schedule_interval='%@hourly',
    start_date=datetime(2022, 9, 20),
) as dag:
    extraction = DummyOperator(task_id='GFUMoron')
    transformation = DummyOperator(task_id='Proc_with_Pandas')
    load = DummyOperator(task_id='Load_data_in_S3')

try:
    extraction
    logger.info("Dag running normally")
except logging.exception:
    logger.error("Problems connecting to database!")

try:
    transformation
    logger.info("Data transforming running normally")
except logging.exception:
    logger.error("Problems transforming data!")

try:
    load
    logger.info("Uploading data running normally")
except logging.exception:
    logger.error("Problems uploading data to S3!")


