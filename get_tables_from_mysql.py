from datetime import datetime, timedelta

import pymysql
import clickhouse_connect

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator


MYSQL_CONN_ID = "tourservice_mysql"
CH_CONN_ID = "clickhouse"

MYSQL_SCHEMA = None
CH_DB = None

TABLE_NAME = "dict32"
RAW_TABLE = "dict32_raw"
OVERLAP_MINUTES = 5


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


def _ch_client():
    conn = BaseHook.get_connection(CH_CONN_ID)
    db = CH_DB or (conn.schema or "default")

    client = clickhouse_connect.get_client(
        host=conn.host,
        port=int(conn.port) if conn.port else 8123,
        username=conn.login or "default",
        password=conn.password or "",
        database=db,
    )
    return db, client


def incremental_load():
    _, ch_client = _ch_client()

    wm_sql = f"""
        SELECT last_changed
        FROM etl_watermarks
        WHERE table_name = '{TABLE_NAME}'
        ORDER BY updated_at DESC
        LIMIT 1
    """
    wm_res = ch_client.query(wm_sql)

    if not wm_res.result_rows:
        raise ValueError(f"Watermark for table {TABLE_NAME} not found in etl_watermarks")

    last_changed = wm_res.result_rows[0][0]
    last_changed_with_overlap = last_changed - timedelta(minutes=OVERLAP_MINUTES)

    _, mysql = _mysql_conn()
    try:
        with mysql.cursor() as cursor:
            query = f"""
                SELECT *
                FROM {TABLE_NAME}
                WHERE changed >= %s
                ORDER BY changed, rid
            """
            cursor.execute(query, (last_changed_with_overlap,))
            rows = cursor.fetchall()
            print(f"Fetched {len(rows)} rows from MySQL")

            for r in rows[:20]:
                print(r)

        if not rows:
            print(f"No new rows for {TABLE_NAME}")
            return

        # clickhouse_connect обычно надежнее ест list[list], чем list[dict]
        columns = list(rows[0].keys())
        data = [[row[col] for col in columns] for row in rows]

        ch_client.insert(
            table=RAW_TABLE,
            data=data,
            column_names=columns,
        )

        max_changed = max(row["changed"] for row in rows)

        ch_client.command(f"""
            INSERT INTO etl_watermarks (table_name, last_changed)
            VALUES ('{TABLE_NAME}', toDateTime('{max_changed:%Y-%m-%d %H:%M:%S}'))
        """)

        print(
            f"Loaded {len(rows)} rows into {RAW_TABLE}. "
            f"Watermark updated to {max_changed:%Y-%m-%d %H:%M:%S}"
        )

    finally:
        mysql.close()


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
    tags=["test", "incremental", "mysql", "clickhouse"],
) as dag:

    load_incremental_dict32 = PythonOperator(
        task_id="load_incremental_dict32",
        python_callable=incremental_load,
    )
