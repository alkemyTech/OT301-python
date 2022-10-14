#Dag de ETL para la Universidad Nacional Del Comahue

#Importamos las librerias que utilizaremos
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import timedelta,datetime
import pandas as pd
import logging
from os import path

# Le damos la configuración base a logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(message)s', 
                    datefmt='%Y/%m/%d')
logging.info('Dags iniciado')               #Creamos el registro de que se inicio el dag

default_args = {
    'retries': 5,                           #Configuramos los retries
    'retry_delay': timedelta(minutes=5)
}
path_dags = path.dirname(path.realpath(__file__)) #Guardo la dirección de este .py
name_un = 'GBUNComahue'                           #Nombre de la universidad
dir_txt = path_dags.replace('/dags',f'/datasets/{name_un}_process.txt') #Dirección del .txt procesado

#Defino la función que conectara a la bd y creará el archivo .csv
def extract():                              
    #Leo el archivo .sql y guardo la sentencia en una variable
    with open(path_dags.replace('/dags',f'/include/{name_un}.sql'),'r') as f:
        query = f.read()                    
        f.close()
    #Realizo la extración
    try:
        pg_hook = PostgresHook(postgres_conn_id='alkemy_db')
        df_data = pg_hook.get_pandas_df(sql=query)
        logging.info('Exporting query to file')
        df_data.to_csv(path_dags.replace('/dags',f'/files/{name_un}_select.csv'))
        logging.info('Exporting finished')
    except:
        logging.warning('Data base - Connection failed')

#Defino la función que procesara los datos de la universidad
def transform():
    #Genero el dataframe base sobre el que transgormare los datos del csv
    df = pd.read_csv(path_dags.replace('/dags',f'/files/{name_un}_select.csv'))
    logging.info('Loaded data')
    #Correción de formato de texto
    df['university'] = df['university'].str.lower()
    df['university'] = df['university'].str.replace('_',' ')
    df['career'] = df['career'].str.lower()
    df['career'] = df['career'].str.replace('_',' ')
    df['last_name'] = df['last_name'].str.lower()
    df['last_name'] = df['last_name'].str.replace('_',' ')
    df['gender'] = df['gender'].str.replace('M','male')
    df['gender'] = df['gender'].str.replace('F','female')
    df['email'] = df['email'].str.lower()
    
    #Genero los datos de la columna 'age'
    df['age'] = pd.to_datetime(df.inscription_date) - pd.to_datetime(df.fecha_nacimiento)
    df['age'] = df.age.astype(int)
    df['age'] = (df.age / (10**9) / 3600 / 24 /365.2425).astype(int)
                #nanoseg   a seg   a h    a d   a años
    
    #Genero el df con el csv de los códigos postales (acá limpiar duplicados perjudica)
    df2 = pd.read_csv(path_dags.replace('/dags',f'/assets/codigos_postales.csv'))
    df2.localidad = df2.localidad.str.lower()
    #Completo la localidad sabiendo los códigos postales
    count = 0
    for x in df.postal_code:
        index_df2 = df2.index[df2['codigo_postal'] == x][0]
        df.location[count] = df2.localidad[index_df2]
        count = count + 1

    #Termino por eliminar columnas no pedidas
    df = df.drop(['Unnamed: 0','fecha_nacimiento'], axis=1)
    logging.info('Processed data')

    #Exporto los resultados a la carpeta y en el formato solicitado
    with open(path_dags.replace('/dags',f'/datasets/{name_un}_process.txt'), 'w') as f:
        dfAsString = df.to_string(header=True, index=False)
        f.write(dfAsString)
        f.close()
    logging.info('Exported data')

#Defino la función que llevará los datos al bucket de aws   
def load(filename=str,key=str,bucket_name=str) -> None:
    logging.info('Upload task started')
    hook = S3Hook('aws_s3_bucket')
    hook.load_file(filename=filename,key=key,bucket_name=bucket_name,replace=True)
    logging.info('Upload task finished')

with DAG(
    'GBUNComahue_dag_etl',
    description = 'ETL para la Universidad Nacional Del Comahue',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,9,19),
    default_args=default_args,
    template_searchpath=path_dags.replace('/dags','/include')
) as dag:
    extract_task = PythonOperator(task_id='extr',python_callable=extract) #Extracción de datos
    transform_task = PythonOperator(task_id='trans',python_callable=transform)  #Procesamiento con Pandas
    load_task = PythonOperator(task_id='load',python_callable=load,op_kwargs={
        'filename':f'{dir_txt}',
        'key':f'{name_un}_process.txt',
        'bucket_name':'cohorte-septiembre-5efe33c6'
    })    #Carga de los datos procesados

    extract_task >> transform_task >> load_task