import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from helpers.utils import fetch_with_retries


@dag(start_date=datetime(2024,1,1), schedule='@daily', catchup=False, tags=['stock_market'])
def stock_market_tickers():
    
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available(ds: str):
        endpoint = "/v3/reference/tickers?type=CS&market=stocks&active=true&limit=1&"
        api = BaseHook.get_connection('stock_api')
        api_key = api.extra_dejson["api_key"]
        url = f"{api.host}{endpoint}&date={ds}&apiKey={api_key}"
        response = requests.get(url=url)
        condition = bool(response.json()['status'] == "OK")
        return condition
    
    @task(retries=5, retry_delay=60)
    def get_stock_tickers(ds: str):
        api = BaseHook.get_connection('stock_api')
        api_key = api.extra_dejson["api_key"]
        endpoint = f"/v3/reference/tickers?type=CS&market=stocks&active=true&limit=1000&date={ds}&apiKey={api_key}"

        url = f"{api.host}{endpoint}"

        # Fetch the initial page
        response = fetch_with_retries(url)
        tickers = response['results']

        # Fetch the next pages until exhausted
        while 'next_url' in response:
            next_url = response['next_url'] + '&apiKey=' + api_key
            response = fetch_with_retries(next_url)
            tickers.extend(response['results'])
        return tickers
    
    @task(retries=5, retry_delay=60)
    def store_stock_tickers():
        pass

    EXEC_DATE = '{{ ds }}'
    print('EXEC_DATE = ' + str(EXEC_DATE))

    # Set task dependencies
    is_api_available(ds=EXEC_DATE) >> get_stock_tickers(ds=EXEC_DATE)

# Run the DAG
stock_market_tickers()