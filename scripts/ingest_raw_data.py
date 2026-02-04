"""
Ingest raw e-commerce CSV data into MySQL raw_orders table.
Executed via Apache Airflow inside Docker.
"""

import pandas as pd
from mysql.connector import Error
import mysql.connector

CSV_PATH = "/opt/project/data/raw/Online-eCommerce.csv"


import os
# Set MYSQL_PASSWORD as an environment variable before running.
DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "host.docker.internal"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE", "de_batch_pipeline")
}


def load_csv_to_sql():
    connection = None
    cursor = None
    try:
        df = pd.read_csv(CSV_PATH)
        print(f"CSV loaded succesfully {len(df)} rows")

        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        print("Connected to MySQL")

        insert_query = """
        INSERT INTO raw_orders (
                order_id, user_id, product_id, category,
                price, qty, total_price, order_date,
                country, customer_segment
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        data = [
            (
                str(row["order_id"]),
                str(row["user_id"]),
                str(row["product_id"]),
                str(row["category"]),
                str(row["price"]),
                str(row["qty"]),
                str(row["total_price"]),
                str(row["order_date"]),
                str(row["country"]),
                str(row["customer_segment"]),

            )
            for _, row in df.iterrows()
        ]

        cursor.executemany(insert_query, data)
        connection.commit()

        print(f"{cursor.rowcount} rows inserted into raw_orders")
    except Error as e:
        print("Error occured: ", e)

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("MySQL connection closed")

if __name__ == "__main__":
    load_csv_to_sql()
