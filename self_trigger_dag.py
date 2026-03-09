from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime
import time


def test_task():
    print("Task started")
    time.sleep(5)
    print("Task finished")


with DAG(
    dag_id="self_trigger_test",
    start_date=datetime(2024, 1, 1),
    schedule=None,        # ВАЖНО: отключаем cron
    catchup=False,
    max_active_runs=1,    # чтобы не было 100 одновременных запусков
    tags=["test", "self_trigger"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_task = PythonOperator(
        task_id="run_task",
        python_callable=test_task
    )

    trigger_self = TriggerDagRunOperator(
        task_id="trigger_self",
        trigger_dag_id="self_trigger_test"
    )

    end = EmptyOperator(task_id="end")

    start >> run_task >> trigger_self >> end
