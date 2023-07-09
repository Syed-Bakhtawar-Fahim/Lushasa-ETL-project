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
    dag_id = 'Lushasa-etl-with-xcom',
    default_args = default_args,
    description = "This is complete ETL of Lushasa(Category: Face)",
    schedule_interval = timedelta(days=1)
)


# Functions to be used:
def lushasa_scrap(url, ti):
    req = requests.get(url)
    soup = BeautifulSoup(req.text, "html.parser")
    data = soup.find_all('div', class_='product w-100 mx-auto mt-0 mt-lg-0')
    data_list = []
    for i in range(len(data)):
        for category in data[i].select("div.product-tile-category"):
            # print(category.get_text())
            categ= category.get_text()

        for title in data[i].select("h3.product-tile-name"):
            # print(title.get_text().replace("\n", ""))
            tit = title.get_text().replace("\n", "")

        for price in data[i].select("div.tile-price-size span.tile-price"):
            # print(price.get_text())
            prc = price.get_text()
        data_dict = (categ, tit, prc)
        data_list.append(data_dict)
        # print(data_list)
        ti.xcom_push(key = "data", value = data_list)

    


def sql_query(ti):
    conn = mysql.connector.connect(
        host = 'host.docker.internal',
        password = "MySQL@Pass",
        user = "root",
        database = "lushasa_database"

    )
    mycursor = conn.cursor()

    data = ti.xcom_pull(task_ids = "lushasa_dataframe", key = "data")

    try:
        insert_stmt = (
        "INSERT INTO lushasa_datascrap_xcom (Category, Title, Price) VALUES (%s, %s, %s)"
        )
        mycursor.executemany(insert_stmt, data)
        conn.commit()
        print("Data inserted")
    
    except mysql.connector.Error as error:
        print(f"Something went wrong due to: {error}")
        conn.rollback()
    conn.close()


# Define the task 

lushasa_dataframe = PythonOperator(
    task_id = "lushasa_dataframe",
    python_callable = lushasa_scrap,
    op_kwargs = {'url': "https://www.lushusa.com/face/cleansers-scrubs/?cgid=cleansers-scrubs&start=0&sz=28"},
    dag = dag
)

insert_into_table = PythonOperator(
    task_id = "insert_into_table",
    python_callable = sql_query,
    dag = dag
)

lushasa_dataframe >> insert_into_table