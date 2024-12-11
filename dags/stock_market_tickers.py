import requests
import boto3
import botocore
import json
import gzip
import io
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime
from helpers.utils import fetch_with_retries


# Import Variables from Airflow
AWS_KEY = Variable.get("AWS_KEY")
AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY")


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
    def store_stock_tickers(tickers, ds):
        s3 = boto3.client("s3", aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
        bucket_name = "stock-tickers"
        key = f"{ds}/tickers.json.gz" # To show that it is json compressed using gzip

        # turn the dictionary into a json
        json_data = json.dumps(tickers)

        try:
            # Create binary buffer to story in compressed bytes in memory   
            buffer = io.BytesIO()
            with gzip.GzipFile(fileobj=buffer, mode='w') as file:
                file.write(json_data.encode('utf-8'))

            # Reset buffer so it can be read from beginning for writing to s3
            buffer.seek(0)

            # Specify compression type
            s3.put_object(Body=buffer, Bucket=bucket_name, Key=key, ContentEncoding='gzip')
        except botocore.exceptions.ClientError as e:
            print(f"Error uploading key: {key}; error: {e}")

    # Set task dependencies
    is_api_available() >> store_stock_tickers(get_stock_tickers())

# Run the DAG
stock_market_tickers()