import os
from datetime import datetime
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

airflow_home = os.environ['AIRFLOW_HOME']
PATH_TO_DBT_PROJECT = f'{airflow_home}/dbt_stocks'
# PATH_TO_DBT_VENV = f'{airflow_home}/dbt_venv/bin/active'
# PATH_TO_DBT_VARS = f'{airflow_home}/dbt_stocks/dbt.env'
# ENTRYPOINT_CMD = f'source {PATH_TO_DBT_VENV} && source {PATH_TO_DBT_VARS}'

@dag(start_date=datetime(2024,1,1), schedule='@daily', catchup=False, tags=['data_warehouse'])
def dbt_run():
    
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='dbt deps',
        cwd=PATH_TO_DBT_PROJECT
    )

    dbt_run_src = BashOperator(
        task_id='dbt_run_src',
        bash_command='dbt run -s tag:src',
        cwd=PATH_TO_DBT_PROJECT
    )

    dbt_run_int_audit = BashOperator(
        task_id='dbt_run_int_audit',
        bash_command='dbt run -s tag:int_audit --vars \'{"run_date": "{{ ds }}"}\'',
        cwd=PATH_TO_DBT_PROJECT
    )

    dbt_run_int_audit_test = BashOperator(
        task_id='dbt_run_int_audit_test',
        bash_command='dbt test -s tag:int_audit',
        cwd=PATH_TO_DBT_PROJECT
    )

    dbt_run_int = BashOperator(
        task_id='dbt_run_int',
        bash_command='dbt run -s tag:int --vars \'{"run_date": "{{ ds }}"}\'',
        cwd=PATH_TO_DBT_PROJECT
    )

    dbt_run_int_test = BashOperator(
        task_id='dbt_run_int_test',
        bash_command='dbt test -s tag:int',
        cwd=PATH_TO_DBT_PROJECT
    )

    dbt_run_snapshot = BashOperator(
        task_id='dbt_run_snapshot',
        bash_command='dbt snapshot',
        cwd=PATH_TO_DBT_PROJECT
    )

    dbt_run_fct = BashOperator(
        task_id='dbt_run_fct',
        bash_command='dbt run -s tag:fct',
        cwd=PATH_TO_DBT_PROJECT
    )


    dbt_deps >> dbt_run_src >> \
        dbt_run_int_audit >> dbt_run_int_audit_test >> dbt_run_int >> dbt_run_int_test >> \
            dbt_run_snapshot >> \
                dbt_run_fct
            

dbt_run()