from datetime import datetime

from airflow.decorators import dag
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

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
        wait_for_completion=False,
    )

    wait_for_stock_government_trades_dag = ExternalTaskSensor(
        task_id="wait_for_stock_government_trades_dag",
        external_dag_id="stock_government_trades",
        # will try this, aliased for task dependency in dag
        external_task_id="sf_copy_government_trades",
        allowed_states=["success"],
        failed_states=["failed", "skipped", "upstream_failed"],
        poke_interval=60,  # every minute
        timeout=1200,  # 20 minutes
    )

    trigger_stock_market_close_dag = TriggerDagRunOperator(
        task_id="trigger_stock_market_close_dag",
        trigger_dag_id="stock_market_close",
        conf={"run_date": "{{ ds }}"},
        wait_for_completion=False,
    )

    wait_for_stock_market_close_dag = ExternalTaskSensor(
        task_id="wait_for_stock_market_close_dag",
        external_dag_id="stock_market_close",
        # will try this, aliased for task dependency in dag
        external_task_id="sf_copy_market_close",
        allowed_states=["success"],
        failed_states=["failed", "skipped", "upstream_failed"],
        poke_interval=60,  # every minute
        timeout=1200,  # 20 minutes
    )

    trigger_stock_market_tickers_dag = TriggerDagRunOperator(
        task_id="trigger_stock_market_tickers_dag",
        trigger_dag_id="stock_market_tickers",
        conf={"run_date": "{{ ds }}"},
        wait_for_completion=False,
    )

    wait_for_stock_market_tickers_dag = ExternalTaskSensor(
        task_id="wait_for_stock_market_tickers_dag",
        external_dag_id="stock_market_tickers",
        # will try this, aliased for task dependency in dag
        external_task_id="sf_merge_stock_tickers",
        allowed_states=["success"],
        failed_states=["failed", "skipped", "upstream_failed"],
        poke_interval=60,  # every minute
        timeout=1200,  # 20 minutes
    )

    trigger_dbt_run_dag = TriggerDagRunOperator(
        task_id="trigger_dbt_run_dag",
        trigger_dag_id="dbt_run",
        conf={"run_date": "{{ ds }}"},
        wait_for_completion=True,
    )

    start_main >> [
        trigger_stock_government_trades_dag,
        trigger_stock_market_close_dag,
        trigger_stock_market_tickers_dag,
    ]

    trigger_stock_government_trades_dag >> wait_for_stock_government_trades_dag

    # Grab tickers before market_close
    (
        trigger_stock_market_tickers_dag
        >> wait_for_stock_market_tickers_dag
        >> trigger_stock_market_close_dag
        >> wait_for_stock_market_close_dag
    )

    [
        wait_for_stock_government_trades_dag,
        wait_for_stock_market_close_dag,
        wait_for_stock_market_tickers_dag,
    ] >> trigger_dbt_run_dag


main()
