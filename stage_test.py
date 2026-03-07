from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

import pymysql
import clickhouse_connect


MYSQL_CONN_ID = "tourservice_mysql"
CH_CONN_ID = "clickhouse"

TABLE_NAME = "dict32"
RAW_TABLE = "dict32_raw"
OVERLAP_MINUTES = 5


def incremental_load():

    # --- ClickHouse connection ---
    ch_conn = BaseHook.get_connection(CH_CONN_ID)
    ch_client = clickhouse_connect.get_client(
        host=ch_conn.host,
        port=ch_conn.port,
        username=ch_conn.login,
        password=ch_conn.password,
        database=ch_conn.schema
    )

    # --- получить watermark ---
    result = ch_client.query(f"""
        SELECT last_changed
        FROM etl_watermarks
        WHERE table_name = '{TABLE_NAME}'
        ORDER BY updated_at DESC
        LIMIT 1
    """)

    last_changed = result.result_rows[0][0]

    # overlap
    last_changed_with_overlap = last_changed - timedelta(minutes=OVERLAP_MINUTES)

    # --- MySQL connection ---
    mysql_conn = BaseHook.get_connection(MYSQL_CONN_ID)

    mysql = pymysql.connect(
        host=mysql_conn.host,
        port=int(mysql_conn.port),
        user=mysql_conn.login,
        password=mysql_conn.password,
        database=mysql_conn.schema,
        cursorclass=pymysql.cursors.DictCursor
    )

    cursor = mysql.cursor()

    query = f"""
        SELECT *
        FROM {TABLE_NAME}
        WHERE changed >= %s
        ORDER BY changed, rid
    """

    cursor.execute(query, (last_changed_with_overlap,))
    rows = cursor.fetchall()

    if not rows:
        print("No new rows")
        return

    # --- вставка в ClickHouse ---
    ch_client.insert(
        RAW_TABLE,
        rows
    )

    # --- новый watermark ---
    max_changed = max(row["changed"] for row in rows)

    ch_client.command(f"""
        INSERT INTO etl_watermarks (table_name, last_changed)
        VALUES ('{TABLE_NAME}', toDateTime('{max_changed}'))
    """)

    print(f"Loaded {len(rows)} rows. New watermark = {max_changed}")


default_args = {
    "owner": "serzhan",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="test_incremental_dict32",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["test", "incremental"],
) as dag:

    load_incremental = PythonOperator(
        task_id="load_incremental_dict32",
        python_callable=incremental_load
    )
