# Libraries import
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import timedelta
import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.mysql.operators.mysql import MySqlOperator
import json
import csv

# Airflow Setup

default_args = {
    'owner': "SyedBakhtawar",
    'start_date': days_ago(0),
    'email': ['bakhtawarfahim10@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id = 'Lushasa-etl-scraping-csv',
    default_args = default_args,
    description = "This is complete ETL of Lushasa(Category: Face)",
    schedule_interval = timedelta(days=1)
)


# Functions to be used:
def lushana_scrap(url):
    req = requests.get(url)
    soup = BeautifulSoup(req.text, "html.parser")
    data = {'Category': [], "Title": [], "Price": []}
    categories = soup.select("div.product-tile-category")
    for category in categories:
        data['Category'].append(category.get_text())
    titles = soup.select("h3.product-tile-name")
    for title in titles:
        data['Title'].append(title.get_text().replace("\n", ""))
    for child in soup.select("div.tile-price-size span.tile-price"):
        data["Price"].append(child.get_text())
    df = pd.DataFrame.from_dict(data)
    # print(data)
    print(df)
    file_to_csv = df.to_csv('lushasa_data1.csv', index=False, header = None)

    


def sql_query():
    conn = mysql.connector.connect(
        host = 'host.docker.internal',
        password = "MySQL@Pass",
        user = "root",
        database = "lushasa_database"

    )
    mycursor = conn.cursor()

    with open('lushasa_data1.csv') as file:
        csv_file = csv.reader(file, delimiter = ",")
        all_values = []
        for row in csv_file:
            value = (row[0], row[1], row[2])
            all_values.append(value)

    try:
        insert_stmt = (
        "INSERT INTO lushasa_datascrap_table (Category, Title, Price) VALUES (%s, %s, %s)"
        )
        mycursor.executemany(insert_stmt, all_values)
        conn.commit()
        print("Data inserted")
    
    except mysql.connector.Error as error:
        print(f"Something went wrong due to: {error}")
        conn.rollback()
    conn.close()


# Define the task 

lushana_dataframe = PythonOperator(
    task_id = "lushana_dataframe",
    python_callable = lushana_scrap,
    op_kwargs = {'url': "https://www.lushusa.com/face/cleansers-scrubs/?cgid=cleansers-scrubs&start=0&sz=28"},
    dag = dag
)

insert_into_table = PythonOperator(
    task_id = "insert_into_table",
    python_callable = sql_query,
    dag = dag
)

lushana_dataframe >> insert_into_table