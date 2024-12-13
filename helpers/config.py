import os
from dotenv import load_dotenv

base_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
load_dotenv(os.path.join(base_dir, ".env"))

AWS_KEY=os.environ.get("AWS_KEY")
AWS_SECRET_KEY=os.environ.get("AWS_SECRET_KEY")

SF_USER=os.environ.get("SF_USER")
SF_PASSWORD=os.environ.get("SF_PASSWORD")
SF_ACCOUNT=os.environ.get("SF_ACCOUNT")
SF_WAREHOUSE=os.environ.get("SF_WAREHOUSE")
SF_DATABASE=os.environ.get("SF_DATABASE")
SF_SCHEMA=os.environ.get("SF_SCHEMA")

SNOWFLAKE_CREDS = {
    "user": SF_USER,
    "password": SF_PASSWORD,
    "account": SF_ACCOUNT,
    "warehouse": SF_WAREHOUSE,
    "database": SF_DATABASE,
    "schema": SF_SCHEMA
}