import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
import logging as log

logging.basicConfig(level=logging.INFO,
                    format=" %(asctime)s - %(name)s - %(message)s",
                    datefmt='%Y-%m-%d',
                    encoding="utf-8")

dir = Path(__file__).resolve().parent.parent.parent
sql_path = f'{dir}/include/'


def extract(file):
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
