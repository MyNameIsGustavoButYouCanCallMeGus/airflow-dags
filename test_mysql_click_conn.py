from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import pymysql
import clickhouse_connect


def test_mysql():
    conn = BaseHook.get_connection("mysql")

    connection = pymysql.connect(
        host=conn.host,
        port=int(conn.port),
        user=conn.login,
        password=conn.password,
        database=conn.schema,
        connect_timeout=5
    )

    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        result = cursor.fetchone()

    print("MySQL OK:", result)


def test_clickhouse():
    conn = BaseHook.get_connection("clickhouse")

    client = clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port,
        username=conn.login,
        password=conn.password,
        database=conn.schema
    )

    result = client.query("SELECT 1")
    print("ClickHouse OK:", result.result_rows)


with DAG(
    dag_id="test_db_connections_serzhan",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"]
) as dag:

    mysql_task = PythonOperator(
        task_id="test_mysql",
        python_callable=test_mysql
    )

    ch_task = PythonOperator(
        task_id="test_clickhouse",
        python_callable=test_clickhouse
    )

    mysql_task >> ch_task
