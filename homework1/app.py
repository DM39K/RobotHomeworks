import requests
import json
import os

from datetime import date, timedelta
import warnings
from requests.exceptions import HTTPError

from config import Config


def app(config, process_date=date.today().strftime('%Y-%m-%d')):

    payload = {"username": config['user'], "password": config['password']}

    auth = requests.post('/'.join([config['url'],'auth']), json = payload)

    token = auth.json()['access_token']

    try:

        dat = {"date": process_date}

        headers={'Authorization': f'JWT {token}'}

        stonks = requests.get('/'.join([config['url'],'out_of_stock']), headers = headers, json = dat)

        if stonks.status_code < 300:

            result = stonks.json()

            os.makedirs(os.path.join(config['directory'], process_date), exist_ok=True)

            with open('/'.join([config['directory'],process_date,'out_of_stock.json']), 'w') as f:

                json.dump(result, f)

        else:

            raise HTTPError(f'{process_date} - Response is not OK - {stonks.status_code}')
        

    except HTTPError as e:
        print(str(e))
    except Exception as e:
        print(type(e))


if __name__ == '__main__':

    warnings.filterwarnings('ignore') 

    config = Config(os.path.join('.', 'config.yaml'))

    dates = [(date.today()-timedelta(x)).strftime('%Y-%m-%d') for x in range(1,30)]

    for dat in dates:

        app(
            config=config.get_config('homework_app')
            , process_date=dat
        )
