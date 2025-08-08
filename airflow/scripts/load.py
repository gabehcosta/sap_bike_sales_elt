import pandas as pd
from sqlalchemy import create_engine

DB_CONFIG = {
    'user': 'admin',
    'password': 'admin',
    'host': 'postgres_dw',
    'port': '5432',
    'db': 'bike_sales_dw'
}

def get_engine():
    user = DB_CONFIG['user']
    password = DB_CONFIG['password']
    host = DB_CONFIG['host']
    port = DB_CONFIG['port']
    db = DB_CONFIG['db']

    url = (
        f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}'
    )

    return create_engine(url)

def upload_dataframe_to_postgres(df: pd.DataFrame, table_name: str, if_exists='replace', schema=None):
    engine = get_engine()

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists=if_exists,
        index=False,
        schema=schema,
        method='multi'
    )

