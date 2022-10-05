
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from pathlib import Path
import pandas as pd

#Setting information loggers
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(module)s - %(message)s',
    datefmt='%Y-%m-%d')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,                                
    'retry_delay': timedelta(minutes=5)
}
#Setting information loggers
sql_path= Path(__file__).resolve().parents[1]
sql_name= 'GCUJujuy'<<<<<<< OT301-63
path_txt=(f'./OT301-python/airflow/datasets/{sql_name}_process.txt')

#Setting the extraction task
def extraccion():
    try:
        f= open(f'{sql_path}/include/{sql_name}.sql','r')
        query = f.read()
        f.close()
        

        pg_hook = PostgresHook(postgres_conn_id='alkemy_db')
        logging.info
        (f"-Exporting {sql_name}.")
        df=pg_hook.get_pandas_df(query)

        df.to_csv(f'./OT301-python/airflow/files/{sql_name}_select.csv', sep=',')

    except:
        logging.warning
        (f"-Exporting {sql_name} did not perform as expected.")

#Setting the transformation task    
def transformacion():
        
    try:
        df_0 = pd.read_csv(f'./OT301-python/airflow/files/{sql_name}_select.csv', sep=',')
        
        
        #Dropping unnecessary columns
        df_0 = df_0.drop(['Unnamed: 0'], axis=1)
        
        #Modifying columns according to the issue
        df_0['last_name']=df_0['last_name'].str.rstrip().str.lstrip()
        df_0['email']=df_0['email'].str.lower().str.replace('-',' ').str.rstrip().str.lstrip()
        df_0['career']=df_0['career'].str.rstrip().str.lstrip()
        df_0['university']= df_0['university'].str.rstrip().str.lstrip()
        df_0['inscription_date']=pd.to_datetime(df_0['inscription_date']).dt.strftime('%Y-%m-%d').astype(str)
        df_0['gender']=df_0['gender'].replace({'f':'female','m':'male'})
        
        try:
            
            #Getting the missing columns from codigos_postales.csv
            
            df_cp = pd.read_csv(f'{sql_path}/assets/codigos_postales.csv')
            
            df_cp.codigo_postal = df_cp.codigo_postal
            
            df_cp['localidad']=df_cp['localidad'].str.lower()
            df_cp = df_cp.drop_duplicates(subset=['localidad'],keep='first')
            
            count = 0
            for x in df_0.location:
                index_df2 = df_cp.index[df_cp['localidad'] == x]
                df_0.postal_code[count] = df_cp.codigo_postal[index_df2]
                count = count + 1
            
            
        except:
            logging.info(f'({df_cp}) could not be loaded')
            pass


        #Age calculation for people over 16 years old. 
        df_0['age']= pd.to_datetime(df_0['inscription_date']) - pd.to_datetime(df_0['birth_date'])
        df_0['age']=df_0['age'].astype(int)
        df_0['age']= (df_0['age'] / (10**9) / 3600 / 24 /365.2425).astype(int)
        df_0['age']= df_0.age.apply(lambda age: age + 0 if (age < 0) else age+18)
        #lambda function is executed since table doesnt take negative values in age. 


        df_0.to_csv(f'./OT301-python/airflow/datasets/{sql_name}_process.txt', sep=',')

        
    except:
        logging.error
        (f"-File not found.")


#Loading task
def cargando():
    
    pass

with DAG(
    dag_id='GCUJujuy_ETL_dag',

    default_args= default_args,
    description= 'ETL Universidad Jujuy',
    schedule_interval= timedelta (hours=1),     
    start_date= datetime.fromisoformat('2022-09-20'), 
    catchup=False
    ) as dag:

#Tasks execution

    extraccion_task = PythonOperator(task_id='extraccion',dag=dag, python_callable= extraccion)
    transformacion_task = PythonOperator(task_id='transformacion',dag=dag, python_callable= transformacion)
    cargando_task  = DummyOperator(task_id='cargando')

    extraccion_task >> transformacion_task >> cargando_task

