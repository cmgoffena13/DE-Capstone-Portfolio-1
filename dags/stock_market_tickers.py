import requests
import os
import boto3
import botocore
import json
import gzip
import io
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from helpers.utils import fetch_with_retries
from helpers.config import SNOWFLAKE_CREDS, AWS_KEY, AWS_SECRET_KEY, SF_DATABASE
from snowflake.snowpark import Session


@dag(start_date=datetime(2024,1,1), schedule_interval=None, catchup=False, tags=['integration'])
def stock_market_tickers():
    
    @task.sensor(poke_interval=5, timeout=30, mode='poke')
    def is_ticker_api_available(ds: str):
        endpoint = "/v3/reference/tickers?active=true&limit=1"
        api = BaseHook.get_connection('stock_api')
        api_key = api.extra_dejson["stock_api_key"]
        url = f"{api.host}{endpoint}&date={ds}&apiKey={api_key}"

        response = requests.get(url=url)
        condition = bool(response.json()['status'] == "OK")
        return condition
    
    @task(retries=5, retry_delay=30)
    def get_stock_tickers(ds: str):
        api = BaseHook.get_connection('stock_api')
        api_key = api.extra_dejson["stock_api_key"]
        endpoint = f"/v3/reference/tickers?active=true&limit=1000&date={ds}&apiKey={api_key}"

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
    
    @task(retries=5, retry_delay=30)
    def store_stock_tickers(tickers, ds):
        s3 = boto3.client("s3", aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
        bucket_name = "polygon-stocks-1"
        key = f"stock-tickers/{ds}_tickers.json.gz" # To show that it is json compressed using gzip

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
        except botocore.exceptions.ClientError as error:
            print(f"Error uploading key: {key}; error: {error}")

    @task(retries=5, retry_delay=30)
    def sf_copy_stock_tickers():
        session = Session.builder.configs(SNOWFLAKE_CREDS).create()

        base_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        copy_into_sql_path = os.path.join(base_dir, "include/snowflake/7_STOCK_TICKERS_COPY_INTO.sql")

        with open(copy_into_sql_path, 'r') as file:
            sql_query = file.read()

        truncate = f"TRUNCATE TABLE {SF_DATABASE}.staging.stock_tickers;"

        session.sql(truncate).collect()
        session.sql(sql_query).collect()
        session.close()

    @task(retries=5, retry_delay=30)
    def sf_merge_stock_tickers():
        session = Session.builder.configs(SNOWFLAKE_CREDS).create()

        merge_sproc = "CALL staging.merge_stock_tickers();"

        session.sql(merge_sproc).collect()
        session.close()


    # Set task dependencies for readability
    api_available = is_ticker_api_available()
    extract = get_stock_tickers()
    store = store_stock_tickers(extract)
    sf_insert = sf_copy_stock_tickers()
    sf_merge = sf_merge_stock_tickers()

    # Set task dependencies
    api_available >> extract >> store >> sf_insert >> sf_merge

# Run the DAG
stock_market_tickers()