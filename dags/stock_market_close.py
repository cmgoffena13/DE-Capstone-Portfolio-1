import requests
import json
import io
import gzip
import boto3
import botocore
import os
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from helpers.config import SNOWFLAKE_CREDS, AWS_KEY, AWS_SECRET_KEY
from snowflake.snowpark import Session
from helpers.utils import fetch_with_retries

# "0 0 * * 2-6"
# Runs Tuesday - Saturday, grabbing the previous day, so grabbing weekdays Mon-Fri
@dag(start_date=datetime(2024,1,1), schedule_interval=None, catchup=False, tags=['integration'])
def stock_market_close():

    @task.sensor(poke_interval=5, timeout=30, mode='poke')
    def is_market_close_api_available(ds: str):
        # Use apple as a default
        endpoint = f"/v1/open-close/AAPL/{ds}?adjusted=true"
        api = BaseHook.get_connection('stock_api')
        api_key = api.extra_dejson["stock_api_key"]
        url = f"{api.host}{endpoint}&apiKey={api_key}"

        response = requests.get(url=url)
        condition = bool(response.status_code in (200, 404))
        return condition
    
    @task(retries=5, retry_delay=30)
    def get_market_close(ds: str):
        api = BaseHook.get_connection('stock_api')
        api_key = api.extra_dejson["stock_api_key"]

        session = Session.builder.configs(SNOWFLAKE_CREDS).create()
        query = """
        SELECT TICKER
        FROM source.STOCK_TICKERS
        WHERE TICKER IN (
        'AMZN'
        )
        """
        tickers_df = session.sql(query)

        tickers = tickers = [row.TICKER for row in tickers_df.collect()]

        records = []
        for ticker in tickers:
            url = f"{api.host}/v1/open-close/{ticker}/{ds}/?adjusted=true&apiKey={api_key}"
            try:
                response = fetch_with_retries(url)
            except requests.exceptions.HTTPError as error:
                if error.response.status_code == 404:
                    print(f"Data not found for ticker: {ticker} on {ds}. Continuing...")
                    continue
                else:
                    raise(error)
            except Exception as catch_all_error:
                raise(catch_all_error)
            
            records.append(response)
        return records

    @task(retries=5, retry_delay=30)
    def store_market_close(records, ds: str):
        s3 = boto3.client("s3", aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
        bucket_name = "polygon-stocks-1"
        key = f"market-close/{ds}_market_close.json.gz"

        json_data = json.dumps(records)

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
    def sf_copy_market_close():
        session = Session.builder.configs(SNOWFLAKE_CREDS).create()

        base_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        copy_into_sql_path = os.path.join(base_dir, "include/snowflake/9_MARKET_CLOSE_COPY_INTO.sql")

        with open(copy_into_sql_path, 'r') as file:
            sql_query = file.read()

        session.sql(sql_query).collect()
        session.close()

    
    api_available = is_market_close_api_available()
    extract = get_market_close()
    store = store_market_close(extract)
    sf_insert = sf_copy_market_close()

    api_available >> extract >> store >> sf_insert

stock_market_close()