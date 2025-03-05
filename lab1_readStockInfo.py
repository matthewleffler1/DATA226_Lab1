from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import datetime, timedelta
import yfinance as yf
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import os


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
def extract(symbols):

    results = []  # List to store stock data

    for symbol in symbols:
        df = yf.download(symbol, period='180d')

        # Remove multi-index 
        df = df.droplevel(0, axis=1)
        # Rename the columns
        df.columns = ['Open', 'Close', 'High', 'Low', 'Volume']
        
        # Reset the index to make 'Date' a column
        df = df.reset_index()
        
        # Convert each row to a dictionary and add the symbol
        for _, row in df.iterrows():
            stock_data = {
                "Symbol": symbol,
                "Date": row['Date'].strftime('%Y-%m-%d'),  # Format date as string
                "Open": row['Open'],
                "Close": row['Close'],
                "High": row['High'],
                "Low": row['Low'],
                "Volume": row['Volume']
            }
            results.append(stock_data)
    
    return results

@task
def load(con, records, target_table):
    try:
        con.execute("BEGIN;")
        con.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
          SYMBOL VARCHAR NOT NULL,
          DT DATE NOT NULL,
          OPEN NUMBER(38,0),
          CLOSE NUMBER(38,0),
          HIGH NUMBER(38,0),
          LOW NUMBER(38,0),
          VOLUME NUMBER(38,0),
          PRIMARY KEY (SYMBOL, DT)
        );""")
        con.execute(f"DELETE FROM {target_table}")

        for i in records:
            sql = f"""
            INSERT INTO {target_table} (SYMBOL, DT, OPEN, CLOSE, HIGH, LOW, VOLUME)
            VALUES (
                '{i["Symbol"]}',
                '{i["Date"]}',
                {i["Open"]},
                {i["Close"]},
                {i["High"]},
                {i["Low"]},
                {i["Volume"]}
            )"""
            con.execute(sql)

        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'lab1_readStockInfo',
    start_date = datetime(2025,3,1),
    catchup=False,
    tags=['ETL'],
    schedule = '0 5 * * *'
) as dag:
    target_table = "dev.raw.lab1_stock_price_table"
    cur = return_snowflake_conn()
    symbols = ["FIVE", "AAPL"]
    # Extract data and combine into a single DataFrame
    extracted_data = extract(symbols)
    # Load the combined data
    load(cur, extracted_data, target_table)
