



def extract():
    file = 'GFUNMoron'
    logger.info(f'Reading file {file}.sql')
    with open (f'{sql_path}/SQL_{file}.sql', 'r') as f:
        query = f.read()
        f. close()

    hook = PostgresHook(postgres_conn_id= 'alkemy_db')

    log.info(f'Execute query {file}.sql')
    pandas_df = hook.get_pandas_df(query)
    print(pandas_df)
