from datetime import datetime
from datetime import timedelta
import json
import requests
import json
import os
import yaml
from requests.exceptions import HTTPError

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator


dates = {'date':['2021-01-01', '2021-10-01', '2021-10-10', '2021-10-26']}


dag_rd = DAG(

    dag_id='oos',

    description='connection to API',

    start_date=datetime(2021,10,26,1,0),
    
    schedule_interval='@daily',

    params=dates

)


def app():
    
    url = "https://robot-dreams-de-api.herokuapp.com/auth"

    headers = {"content-type": "application/json"}

    data = {"username": "rd_dreams", "password": "djT6LasE"}

    r = requests.post(url, headers=headers, data=json.dumps(data))

    token = r.json()['access_token']

    for date in dag_rd.params['date']:

        try:

            os.makedirs(os.path.join("./data/", date), exist_ok=True)

            url = "https://robot-dreams-de-api.herokuapp.com/out_of_stock"

            headers = {"content-type": "application/json", "Authorization": "JWT " + token}

            d = {"date": date}

            response = requests.get(url, headers=headers, data=json.dumps(d))

            response.raise_for_status()

            data = response.json()

            with open(os.path.join("./data/", date, f'Product[{date}].json'), 'w') as f:

                json.dump(data, f)

        except HTTPError as e:
            
            print(str(e))


connection_to_api = PythonOperator(

    task_id='get_data_from_API',

    python_callable=app,

    dag=dag_rd

)


connection_to_api