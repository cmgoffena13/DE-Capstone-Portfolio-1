from datetime import datetime

from airflow.decorators import dag
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {"depends_on_past": False}


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
        conf={"run_date": "{{ ds }}"},
        wait_for_completion=True,
    )

    trigger_stock_market_close_dag = TriggerDagRunOperator(
        task_id="trigger_stock_market_close_dag",
        trigger_dag_id="stock_market_close",
        conf={"run_date": "{{ ds }}"},
        wait_for_completion=True,
    )

    trigger_stock_market_tickers_dag = TriggerDagRunOperator(
        task_id="trigger_stock_market_tickers_dag",
        trigger_dag_id="stock_market_tickers",
        conf={"run_date": "{{ ds }}"},
        wait_for_completion=True,
    )

    trigger_dbt_run_dag = TriggerDagRunOperator(
        task_id="trigger_dbt_run_dag",
        trigger_dag_id="dbt_run",
        conf={"run_date": "{{ ds }}"},
        wait_for_completion=True,
    )

    start_main >> [trigger_stock_government_trades_dag, trigger_stock_market_tickers_dag]

    [trigger_stock_government_trades_dag, trigger_stock_market_tickers_dag] >> \
        trigger_stock_market_close_dag

    trigger_stock_market_close_dag >> trigger_dbt_run_dag


main()
