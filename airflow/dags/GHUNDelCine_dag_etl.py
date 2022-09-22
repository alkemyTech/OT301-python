from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

#
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'University_Of_Cinema',  # Dagger name
        default_args=default_args,  # This will automatically apply it to any operators bound to it
        description='ETL DAG for University H data',  # Dags description
        start_date=datetime.now(),  # Dag boot date
        schedule=timedelta(hours=1),  # The dag is going to run every 1 hour
        catchup=False

) as dag:
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
        dag=dag
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
        dag=dag
    )

t1 >> t2
