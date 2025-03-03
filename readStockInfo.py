# In Cloud Composer, add snowflake-connector-python to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():

    user_id = Variable.get('snowflake_userid')
    password = Variable.get('snowflake_password')
    account = Variable.get('snowflake_account')

    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,  # Example: 'xyz12345.us-east-1'
        warehouse='DHK_WH',
        database='dev'
    )
    # Create a cursor object
    return conn.cursor()


@task
def extract(symbol):
    df = yf.download(symbol, period='180d')

    # Remove multi-index
    df = df.droplevel(0, axis=1)
    # Rename the columns
    df.columns = ['Open', 'Close', 'High', 'Low', 'Volume']
    
    # Reset the index to make 'Date' a column
    df = df.reset_index()
    
    # Select only the desired columns
    df = df[['Date', 'Open', 'Close', 'High', 'Low', 'Volume']]
    
    df.insert(0, 'Symbol', symbol)

    return df

@task
def load(con, records, target_table):
    try:
        con.execute("BEGIN;")
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            symbol varchar NOT NULL,
            date timestamp_ntz NOT NULL,
            open float,
            high float,
            low float,
            close float,
            volume number,
            PRIMARY KEY (symbol, date)
        );""")
        con.execute(f"DELETE FROM {target_table}")

    for index, row in data.iterrows():
        sql = f"INSERT INTO {staging_table} (symbol, date, open, high, low, close, volume) VALUES ('{row['Symbol']}', '{row['Date']}', '{row['Open']}', '{row['Close']}', '{row['High']}', '{row['Low']}', '{row['Volume']}')"
        print(sql)
        con.execute(sql)

        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'CountryCaptial',
    start_date = datetime(2025,2,28),
    catchup=False,
    tags=['ETL'],
    schedule = '30 8 * * *'
) as dag:
    target_table = "dev.raw.lab1_stock_price_table"
    cur = return_snowflake_conn()
    symbols = ["FIVE", "AAPL"]
    for symbol in symbols
        data = extract(symbol)
        load(cur, data, target_table)
