from datetime import datetime

from airflow.decorators import dag
from airflow.exceptions import AirflowSkipException
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

default_args = {"depends_on_past": False}


def check_triggered_dag_result(ti):
    result = ti.xcom_pull(
        dag_id="stock_government_trades",  # Specify the triggered DAG ID
        task_ids="is_government_data_available",  # Task name from the triggered DAG
        key="data_available"
    )
    print(result)
    if result is None:
        raise Exception("Xcom 'data available' not received")
    if not result:
        raise AirflowSkipException("No data available, skipping pipeline")


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
)
def main():

    start_main = DummyOperator(task_id="start_main")

    trigger_stock_government_trades_dag = TriggerDagRunOperator(
        task_id="trigger_stock_government_trades_dag",
        trigger_dag_id="stock_government_trades",
        # conf={"run_date": "{{ ds }}"},
        wait_for_completion=True,
        logical_date="{{ ds }}",
        poke_interval=30,
        trigger_run_id="{{ run_id }}"
    )

    check_data_availability = PythonOperator(
        task_id="check_triggered_dag_result",
        python_callable=check_triggered_dag_result,
        provide_context=True,
    )

    trigger_stock_market_close_dag = TriggerDagRunOperator(
        task_id="trigger_stock_market_close_dag",
        trigger_dag_id="stock_market_close",
        # conf={"run_date": "{{ ds }}"},
        wait_for_completion=True,
        logical_date="{{ ds }}",
        poke_interval=30,
        trigger_run_id="{{ run_id }}"
    )

    trigger_stock_market_tickers_dag = TriggerDagRunOperator(
        task_id="trigger_stock_market_tickers_dag",
        trigger_dag_id="stock_market_tickers",
        # conf={"run_date": "{{ ds }}"},
        wait_for_completion=True,
        logical_date="{{ ds }}",
        poke_interval=30,
        trigger_run_id="{{ run_id }}"
    )

    trigger_dbt_run_dag = TriggerDagRunOperator(
        task_id="trigger_dbt_run_dag",
        trigger_dag_id="dbt_run",
        # conf={"run_date": "{{ ds }}"},
        wait_for_completion=True,
        logical_date="{{ ds }}",
        poke_interval=30,
        trigger_run_id="{{ run_id }}"
    )

    (
        start_main
        >> trigger_stock_government_trades_dag
        >> check_data_availability
        >> trigger_stock_market_tickers_dag
    )

    [
        trigger_stock_government_trades_dag,
        trigger_stock_market_tickers_dag,
    ] >> trigger_stock_market_close_dag

    trigger_stock_market_close_dag >> trigger_dbt_run_dag


main()
