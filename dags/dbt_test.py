import os
from datetime import datetime
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

airflow_home = os.environ['AIRFLOW_HOME']
PATH_TO_DBT_PROJECT = f'{airflow_home}/dbt_stocks'
# PATH_TO_DBT_VENV = f'{airflow_home}/dbt_venv/bin/active'
# PATH_TO_DBT_VARS = f'{airflow_home}/dbt_stocks/dbt.env'
# ENTRYPOINT_CMD = f'source {PATH_TO_DBT_VENV} && source {PATH_TO_DBT_VARS}'

@dag(start_date=datetime(2024,1,1), schedule='@daily', catchup=False)
def dbt_test():
    
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='dbt deps',
        cwd=PATH_TO_DBT_PROJECT
    )

    dbt_run_model = BashOperator(
        task_id='dbt_run_model',
        bash_command='dbt run',
        cwd=PATH_TO_DBT_PROJECT
    )


    dbt_deps >> dbt_run_model

dbt_test()