import snowflake.connector
import pandas as pd

#TODO: Change path to CSV file
path = "PATH/TO/CSV"

# TODO: Change login information
conn = snowflake.connector.connect(
    user="USERNAME",
    password="PASSWORD",
    account="ACCOUNT",
    warehouse="WAREHOUSE",
    database="DATABASE",
    schema="SCHEMA",
    role="ROLE"  # Optional
)

# TODO: Change id, car, and price to column names and types
create_table_query = """
CREATE TABLE IF NOT EXISTS my_table (
    id INT,
    car STRING,
    price INT
);
"""

cursor = conn.cursor()
cursor.execute(create_table_query)
cursor.close()

df = pd.read_csv(path)

cursor = conn.cursor()


# TODO: Change id, car, and price to column names
for _, row in df.iterrows():
    cursor.execute(
        "INSERT INTO my_table (id, car, price) VALUES (%s, %s, %s)", 
        (row["id"], row["car"], row["price"])
    )

cursor.close()
conn.commit()

