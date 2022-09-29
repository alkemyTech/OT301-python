from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging as log
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Files
dir = Path(__file__).resolve().parent.parent
sql_path = f'{dir}/include/'
file = 'GHUNDelCine'

# We configure the registers
log.basicConfig(
    level=log.INFO,
    format='%(asctime)s - %(module)s - %(message)s',
    datefmt='%Y-%m-%d'
)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def extract():
    """
    Function that is responsible for extracting the data, from the include folder of the group h
    """

    # Read the sql query, which is in the include folder
    try:
        log.info(f'Reading file {file}.sql')
        with open(f'{sql_path}{file}.sql', 'r') as f:
            query = f.read()
            f.close()
    except Exception as e:
        log.error(f'There was an error reading the query: {e}')

    hook = PostgresHook(postgres_conn_id='alkemy_db')

    # Execute query
    log.info(f'Execute query {file}.sql')
    pandas_df = hook.get_pandas_df(query)

    # Save it as csv
    log.info(f'Saving data in {file}.csv')
    csv_path = f'{dir}/files/{file}_select.csv'
    pandas_df.to_csv(csv_path, sep=',', index=False)

    log.info('Extraction finished')


def transform():
    """
    Function that is responsible for transforming the data
    """
    log.info('Transforming data')
    df = normalize(file)
    log.info('Calculating age')
    df = calculate_age(df, file)
    log.info('Creating locality column')
    df = postal_code_or_location(df, 'postal_code')
    log.info(f'Saving file to {file}.txt')
    df = save_df_text(df)
    log.info('Saved data')


def normalize(file: str) -> str:
    """ The function reads the csv GHUNDeBuenosAires_select and Normalizes all the information Creates a.
        txt of the university of Buenos Aires, obtained from the csv
    """
    
    try:
        log.info(f'Normalizing csv file data from files folder {file}')
        df = pd.read_csv(f'{dir}/files/GHUNDelCine_select.csv')
    except Exception as e:
        log.error(f'file not found: {e}')
        raise e

    # Create a variable with the columns to use
    columns = ['university', 'career',
               'last_name', 'gender', 'location', 'email']

    # Create an iteration for the columns
    for column in columns:
        # convert the dataframe to lowercase
        df[f'{column}'] = df[f'{column}'].str.lower()
        # Replace the "-" in the dataframe with a space
        df[f'{column}'] = df[f'{column}'].apply(lambda x: x.replace('-', ' '))

    # Replace values ​​of 'm' and 'f' with 'male' and 'female' in the gender column
    df['gender'] = df.gender.replace({'f': 'female', 'm': 'male'})

    # We clean the last_name column so that we only have the names and surnames
    abreviations = ["mrs\.", "mr\.", "dr\.", "ms\.", "md",
                "dds", "dvm", "iii", "phd", "jr\.", "ii", "iv", "miss"]

    df['last_name'] = df['last_name'].replace(
        abreviations, value='', regex=True
    )
    # Separate first and last name from the Last_name column, add them to the df and remove the Last_name column
    df = df.drop(['first_name'], axis=1)
    df = df.loc[:,~df.columns.duplicated()]
    return df

def calculate_age(df: pd.DataFrame, file: str) -> pd.DataFrame:
    """Create a column called 'age', make the difference between the 'inscription_date' columns
    and 'birth_date' to get the age in days. With age in days makes a transformation
    in years with the calculate function

    Args:
        df (pd.DataFrame): DataFrame
        file (str): Take as argument to the CSV file

    Returns:
        pd.DataFrame: Transformed DataFrame
    """

    def calculate(diff_days):
        """
        Get days difference, determine if they are negative or positive.
        If they are negative, increment 100 years and return the division between days and years (age).
        If they are positive, it only returns the division
        """
        days = diff_days.days
        if days < 0:
            days += int(100 * 365.2425)

        return int(days / 365.2425)
    if file == 'GHUNDelCine':
        df['inscription_date'] = pd.to_datetime(df.inscription_date, format='%d-%m-%Y')
        df['birth_date'] = pd.to_datetime(df.birth_date, format='%d-%m-%Y')
        df['age'] = df['inscription_date'] - df['birth_date']

    elif file == 'GHUNDeBuenosAires':
        df['inscription_date'] = pd.to_datetime(df.inscription_date)
        df['birth_date'] = pd.to_datetime(df.birth_date, format='%y-%b-%d')
        df['age'] = df['inscription_date'] - df['birth_date']

    df['age'] = df['age'].apply(calculate)
    return df




def postal_code_or_location(df: pd.DataFrame, postal_code_or_location: str) -> pd.DataFrame:
    """Receives a dataframe and a string, the string defines whether to
        add location, or postal_code, it only accepts those two values.

    Args:
        df (pd.DataFrame): DataFrame
        postal_code_or_location (str): location or postal_code

    Returns:
        pd.DataFrame: Return the dataframe
    """
    try:
        log.info('Reading zip code file from assets folder')
        open_data = pd.read_csv(f'{dir}/assets/codigos_postales.csv')
    except OSError as e:
        log.error(f'File not found {e}')

    if postal_code_or_location == 'location':
        # Obteniendo localidad con codigo_postal
        open_data["localidad"] = open_data['localidad'].apply(lambda x: x.lower())
        dict_cp = dict(zip(open_data['codigo_postal'], open_data['localidad']))
        df['location'] = df['postal_code'].apply(lambda x: dict_cp[x])

    elif postal_code_or_location == 'postal_code':
        # Obteniendo codigo postal con localidad
        open_data['localidad'] = open_data["localidad"].apply(lambda x: x.lower())
        dict_cp = dict(zip(open_data['localidad'], open_data['codigo_postal']))
        df['postal_code'] = df['location'].apply(lambda x: dict_cp[x])

    return df


def save_df_text(df):
    """
    Save the dataframe in txt format. in the datasets folder
    """
    try:
        log.info('Saving txt file, in the datasets folder')
        with open(f'{dir}/datasets/{file}_process.txt', 'w+') as f:
            f.write(df.to_string())
            f.close()
    except OSError as e:
        log.error(f'Directory not found: {e}')


def load(filename:str, key:str, bucket_name:str):
    """
    Function that is responsible for uploading the data to amazon s3
    """
    log.info(f'Uploading file to s3 {filename}')
    hook = S3Hook('')
    log.info('Uploading file')
    hook.load_file(
        filename=filename,
        key=key,
        bucket_name=bucket_name,
        replace=True
    )
    log.info('File uploaded to s3 successfully')


with DAG(
        'University_Of_Cinema',  # Dagger name
        default_args=default_args,  # This will automatically apply it to any operators bound to it
        description='ETL DAG for University H data',  # Dags description
        start_date=datetime(2022, 9, 20),  # Dag boot date
        schedule=timedelta(hours=1),  # The dag is going to run every 1 hour
        catchup=False

) as dag:
    t1 = PythonOperator(
        task_id='Get_data',
        dag=dag,
        python_callable=extract

    )

    t2 = PythonOperator(
        task_id='transforming_data',
        dag=dag,
        python_callable=transform
    )

    t3 = PythonOperator(
        task_id='Uploading_to_s3',
        dag=dag,
        python_callable=load,
        op_kwargs={
            'filename':  f'{dir}/datasets/GHUNDeBuenosAires_process.txt',
            'key': f'{dir}_process.txt',
            'bucket_name': 'cohorte-septiembre-5efe33c6'
        }
    )

t1 >> t2 >> t3
