from airflow import models
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import psycopg2
from datetime import datetime
from datetime import timedelta
import os


pg_creds = {
    'host': '192.168.56.1'
    , 'port': '5432'
    , 'database': 'dshop'
    , 'user': 'pguser'
    , 'password': 'secret'
}

tables = ['aisles', 'clients', 'departments', 'orders', 'products']


def copy_to_file():

    for table in tables:

        with psycopg2.connect(**pg_creds) as pg_connection:

            cursor = pg_connection.cursor()

            with open(file=os.path.join('.', 'data', f'{table}.csv'), mode='w') as csv_file:

                cursor.copy_expert(f'COPY public.{table} TO STDOUT WITH HEADER CSV', csv_file)


dag_rd = DAG(

    dag_id='postgres',

    description='copy data to file',

    start_date=datetime(2021,10,26,1,0),

    schedule_interval='@daily'

)


copy_data = PythonOperator(

    task_id="copy_to_file",

    dag=dag_rd,

    python_callable=copy_to_file,

)

copy_data