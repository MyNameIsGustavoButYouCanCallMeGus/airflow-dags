import os
import time
from datetime import datetime, timedelta

import pymysql

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

os.environ["TZ"] = "UTC"
time.tzset()

MYSQL_CONN_ID = "tourservice_mysql"
MYSQL_SCHEMA = None


def _mysql_conn():
    conn = BaseHook.get_connection(MYSQL_CONN_ID)
    schema = MYSQL_SCHEMA or conn.schema

    connection = pymysql.connect(
        host=conn.host,
        port=int(conn.port),
        user=conn.login,
        password=conn.password,
        database=schema,
        connect_timeout=60,
        read_timeout=600,
        write_timeout=600,
        charset="utf8",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
    return schema, connection


def check_changed_range(table_name: str):
    print(f"=== START check table: {table_name} ===")

    _, mysql = _mysql_conn()

    try:
        with mysql.cursor() as cursor:
            query = f"""
                select
                    min(changed) as min_changed,
                    max(changed) as max_changed,
                    count(*) as row_count
                from {table_name}
            """
            cursor.execute(query)
            row = cursor.fetchone()

            print(f"Table: {table_name}")
            print(f"min_changed: {row['min_changed']}")
            print(f"max_changed: {row['max_changed']}")
            print(f"row_count: {row['row_count']}")

    finally:
        mysql.close()

    print(f"=== END check table: {table_name} ===")


default_args = {
    "owner": "serzhan",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="check_mysql_changed_ranges_serzhan",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["mysql", "debug", "changed"],
) as dag:

    check_dict90 = PythonOperator(
        task_id="check_dict90_changed_range",
        python_callable=check_changed_range,
        op_kwargs={"table_name": "dict90"},
    )

    check_dict32 = PythonOperator(
        task_id="check_dict32_changed_range",
        python_callable=check_changed_range,
        op_kwargs={"table_name": "dict32"},
    )

    check_dict90 >> check_dict32
