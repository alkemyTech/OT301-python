
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from airflow.operators.dummy import DummyOperator

from pathlib import Path
import pandas as pd

#Setting information loggers
logging.basicConfig(

    level=logging.info,
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


#Setting file paths
sql_path= Path(__file__).resolve().parents[1]
sql_name= 'GCUPalermo'
path_txt=(f'./OT301-python/airflow/datasets/{sql_name}_process.txt')



#Setting the extraction task
def extraccion():
    with open(f'{sql_path}/include/{sql_name}.sql', 'r') as f:
        query = f.read()
        

    try:
        pg_hook = PostgresHook(postgres_conn_id='alkemy_db')
        logging.info
        (f"-Exporting {sql_name}.")
        df=pg_hook.get_pandas_df(query)

        df.to_csv(f'./OT301-python/airflow/files/{sql_name}_select.csv', sep=',')
    except:
        logging.warning
        (f"-Exporting {sql_name} did not perform as expected.")
        

    
    
#Transformation task, to set
def transformacion():
        
#Reading .csv files
    try:
        df_0 = pd.read_csv(f'./OT301-python/airflow/files/{sql_name}_select.csv', sep=',')
        
        
        try:
            df_cp = pd.read_csv(f'{sql_path}/assets/codigos_postales.csv')
            df_cp = df_cp['localidad'].str.lower().str.rstrip().str.lstrip()
            df_cp = df_cp.rename({'localidad':'location'})
            df_cp= df_cp.drop['codigos_postales']
            df_cp= df_cp['location']
            df_cp=pd.DataFrame(['location'])
            location= df_cp['location']
            df_0 = df_0.join(location)
            
        except:
            logging.info(f'({df_cp}) could not be loaded')
            pass

        #Dropping unnecessary columns
        df_0 = df_0.drop(['Unnamed: 0'], axis=1)
        
        #Modifying columns according to the issue
        df_0['last_name']=df_0['last_name'].str.replace('_',' ').str.rstrip().str.lstrip()
        df_0['email']=df_0['email'].str.lower().str.rstrip().str.lstrip()
        df_0['career']=df_0['career'].str.replace('_',' ').str.rstrip().str.lstrip()
        df_0['university']= df_0['university'].str.replace('_',' ').str.rstrip().str.lstrip()
        df_0['inscription_date']=pd.to_datetime(df_0['inscription_date']).dt.strftime('%Y-%m-%d').astype(str)
        df_0['inscription_date']= df_0['inscription_date'].str.replace('-', '/')
        df_0['gender']=df_0['gender'].replace({'f':'female','m':'male'})
        
        #Age calculation for people over 18 years old. 
        df_0['age']= pd.to_datetime(df_0['inscription_date']) - pd.to_datetime(df_0['birth_dates'])
        df_0['age']=df_0['age'].astype(int)
        df_0['age']= (df_0['age'] / (10**9) / 3600 / 24 /365.2425).astype(int)
        df_0['age'] = df_0.age.apply(lambda age: age + 100 if (age < 0) else age+18)
        #Above, the lambda function is executed, and in this table, if the value is negative, gets it positive to work with it.

        df_0.to_csv(f'./OT301-python/airflow/datasets/{sql_name}_process.txt', sep=',')
    except:
        logging.error
        (f"-File not found.")
        



        try:
            
            #Getting the missing columns from codigos_postales.csv

            df_cp = pd.read_csv(f'{sql_path}/assets/codigos_postales.csv')
            
            df_cp.localidad = df_cp.localidad.str.lower()
            
            count = 0
            for x in df_0.postal_code:
                index_df2 = df_cp.index[df_cp['codigo_postal'] == x][0]
                df_0.location[count] = df_cp.localidad[index_df2]
                count = count + 1
            
            
        except:
            logging.info(f'({df_cp}) could not be loaded')
            pass

        #Age calculation for people over 18 years old. 
        df_0['age']= pd.to_datetime(df_0['inscription_date']) - pd.to_datetime(df_0['birth_dates'])
        df_0['age']= df_0['age'].astype(int)
        df_0['age']= (df_0['age'] / (10**9) / 3600 / 24 /365.2425).astype(int)
        df_0['age']= df_0.age.apply(lambda age: age + 100 if (age < 0) else age+18)
        #Above, the lambda function is executed, and in this table, if the value is negative, gets it positive to work with it.

        df_0.to_csv(f'./OT301-python/airflow/datasets/{sql_name}_process.txt', sep=',')
    except:
        logging.error
        (f"-File not found.")
        

#Loading task

def cargando():
    # data load corresponding to the university received as a parameter
    pass
with DAG(

    'GCUPAlermo3_ETL_dag.py',

    default_args= default_args,
    description= 'ETL Universidad Palermo',
    schedule_interval= timedelta (hours=1),     
    start_date= datetime.fromisoformat('2022-09-20'), 
    catchup=False
    ) as dag:


#Ejecucion de tareas
    extraccion_task = PythonOperator(task_id='extraccion', python_callable= extraccion)
    transformacion_task = PythonOperator(task_id='transformacion', python_callable= transformacion)
    cargando_task = DummyOperator(task_id='cargando')


    extraccion_task >> transformacion_task >> cargando_task

