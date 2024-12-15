import requests
import json
import io
import os
import gzip
import botocore
import boto3
from datetime import datetime
from helpers.utils import fetch_with_retries
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from helpers.config import SNOWFLAKE_CREDS, AWS_KEY, AWS_SECRET_KEY, SF_DATABASE, SF_SCHEMA
from snowflake.snowpark import Session


@dag(start_date=datetime(2024, 1, 1), schedule='@daily', catchup=True, tags=['stock_market'])
def stock_government_trades():
    
    @task.sensor(poke_interval=5, timeout=30, mode='poke')
    def is_government_api_available(ds: str):
        endpoint = "/api/v1/gov/usa/congress/trades?pagesize=1"
        api = BaseHook.get_connection('government_api')
        api_key = api.extra_dejson["government_api_key"]
        url = f"{api.host}{endpoint}&date={ds}&token={api_key}"

        response = requests.get(url=url)
        condition = bool(response.status_code == 200 and str(response.text) != "[]")
        return condition
    
    @task(retries=5, retry_delay=60)
    def get_government_trades(ds: str):
        api = BaseHook.get_connection('government_api')
        api_key = api.extra_dejson["government_api_key"]
        endpoint = f"/api/v1/gov/usa/congress/trades?pagesize=1000&date={ds}&token={api_key}"

        url = f"{api.host}{endpoint}"

        # Fetch the initial page
        response = fetch_with_retries(url)
        trades = response['data']

        # Fetch the next pages until exhausted
        while True:
            page_number = 1
            next_url = url + f"&page={page_number}"
            response = fetch_with_retries(next_url)

            if str(response) == "[]":
                break
            
            trades.extend(response['data'])
            page_number += 1

        return trades

    @task(retries=5, retry_delay=60)
    def store_government_trades(trades, ds):
        s3 = boto3.client("s3", aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
        bucket_name = "government-trades"
        key = f"{ds}_government_trades.json.gz" # To show that it is json compressed using gzip

        # turn the dictionary into a json
        json_data = json.dumps(trades)

        try:
            # Create binary buffer to story in compressed bytes in memory   
            buffer = io.BytesIO()
            with gzip.GzipFile(fileobj=buffer, mode='w') as file:
                file.write(json_data.encode('utf-8'))

            # Reset buffer so it can be read from beginning for writing to s3
            buffer.seek(0)

            # Specify compression type
            s3.put_object(Body=buffer, Bucket=bucket_name, Key=key, ContentEncoding='gzip')
        except botocore.exceptions.ClientError as error:
            print(f"Error uploading key: {key}; error: {error}")

    @task(retries=5, retry_delay=60)
    def sf_copy_government_trades():
        session = Session.builder.configs(SNOWFLAKE_CREDS).create()

        base_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        copy_into_sql_path = os.path.join(base_dir, "include/snowflake/10_GOVERNMENT_TRADES_COPY_INTO.sql")

        with open(copy_into_sql_path, 'r') as file:
            sql_query = file.read()

        session.sql(sql_query).collect()
        session.close()


    api_available = is_government_api_available()
    extract = get_government_trades()
    store = store_government_trades(extract)
    sf_insert = sf_copy_government_trades()

    api_available >> extract >> store >> sf_insert

stock_government_trades()