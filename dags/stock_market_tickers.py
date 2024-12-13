import requests
import boto3
import botocore
import json
import gzip
import io
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from helpers.utils import fetch_with_retries
from helpers.config import SNOWFLAKE_CREDS, AWS_KEY, AWS_SECRET_KEY, SF_DATABASE, SF_SCHEMA
from snowflake.snowpark import Session


@dag(start_date=datetime(2024,1,1), schedule='@monthly', catchup=False, tags=['stock_market'])
def stock_market_tickers():
    
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
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
        except botocore.exceptions.ClientError as error:
            print(f"Error uploading key: {key}; error: {error}")
        return (bucket_name, key)

    @task(retries=5, retry_delay=30)
    def sf_insert_stock_tickers(address):
        bucket_name, key = address
        s3 = boto3.client("s3", aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
        tickers = s3.get_object(Bucket=bucket_name, Key=key)

        with gzip.GzipFile(fileobj=tickers["Body"]) as gzipped_file:
            data = json.load(gzipped_file)

        session = Session.builder.configs(SNOWFLAKE_CREDS).create()

        df = session.create_dataframe(data)

        table_name = "stock_tickers"

        # trigger sql statements using collect
        session.sql(f"USE DATABASE {SF_DATABASE}").collect()
        session.sql(f"USE SCHEMA {SF_SCHEMA}").collect()

        # Check system tables for existence
        result = session.sql(f"SHOW TABLES LIKE '{table_name}'").collect()

        # Check if the table exists, create it only if it doesn't
        if result:
            df.write.mode("overwrite").save_as_table(table_name)
        else:
            df.write.mode("ignore").save_as_table(table_name)

    # Set task dependencies for readability
    api_available = is_ticker_api_available()
    extract = get_stock_tickers()
    store = store_stock_tickers(extract)
    sf_insert = sf_insert_stock_tickers(store)

    # Set task dependencies
    api_available >> extract >> store >> sf_insert

# Run the DAG
stock_market_tickers()