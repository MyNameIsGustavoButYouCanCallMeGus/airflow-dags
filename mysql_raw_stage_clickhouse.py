from datetime import datetime, timedelta

import pymysql
import clickhouse_connect

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


MYSQL_CONN_ID = "tourservice_mysql"
CH_CONN_ID = "clickhouse"

MYSQL_SCHEMA = None
CH_DB = None

OVERLAP_MINUTES = 5
DEBUG_SAMPLE_ROWS = 20


TABLES = [
    {"table_name": "dict13", "raw_table": "dict13_raw", "stage_table": "dict13_stage"},
    {"table_name": "dict14", "raw_table": "dict14_raw", "stage_table": "dict14_stage"},
    {"table_name": "dict15", "raw_table": "dict15_raw", "stage_table": "dict15_stage"},
    {"table_name": "dict59", "raw_table": "dict59_raw", "stage_table": "dict59_stage"},
    {"table_name": "dict31", "raw_table": "dict31_raw", "stage_table": "dict31_stage"},
    {"table_name": "dict32", "raw_table": "dict32_raw", "stage_table": "dict32_stage"},
    {"table_name": "dict90", "raw_table": "dict90_raw", "stage_table": "dict90_stage"},
    {"table_name": "dict91", "raw_table": "dict91_raw", "stage_table": "dict91_stage"},
    {"table_name": "dict3", "raw_table": "dict3_raw", "stage_table": "dict3_stage"},
    {"table_name": "dict4", "raw_table": "dict4_raw", "stage_table": "dict4_stage"}
]


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


def _get_watermark(ch_client, table_name: str):
    wm_sql = f"""
        SELECT last_changed
        FROM etl_watermarks
        WHERE table_name = '{table_name}'
        ORDER BY updated_at DESC
        LIMIT 1
    """
    wm_res = ch_client.query(wm_sql)

    if not wm_res.result_rows:
        raise ValueError(f"Watermark for table {table_name} not found in etl_watermarks")

    return wm_res.result_rows[0][0]


def _insert_watermark(ch_client, table_name: str, max_changed):
    ch_client.command(f"""
        INSERT INTO etl_watermarks (table_name, last_changed)
        VALUES ('{table_name}', toDateTime('{max_changed:%Y-%m-%d %H:%M:%S}'))
    """)


def incremental_load_table(table_name: str, raw_table: str):
    print(f"=== START incremental load: {table_name} -> {raw_table} ===")

    _, ch_client = _ch_client()

    last_changed = _get_watermark(ch_client, table_name)
    last_changed_with_overlap = last_changed - timedelta(minutes=OVERLAP_MINUTES)

    print(f"Watermark: {last_changed}")
    print(f"Overlap watermark: {last_changed_with_overlap}")

    _, mysql = _mysql_conn()

    try:
        with mysql.cursor() as cursor:
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE changed >= %s
                ORDER BY changed, rid
            """
            cursor.execute(query, (last_changed_with_overlap,))
            rows = cursor.fetchall()

        print(f"Fetched {len(rows)} rows from MySQL for {table_name}")

        for r in rows[:DEBUG_SAMPLE_ROWS]:
            print(r)

        if not rows:
            print(f"No new rows for {table_name}")
            return

        columns = list(rows[0].keys())
        data = [[row[col] for col in columns] for row in rows]

        ch_client.insert(
            table=raw_table,
            data=data,
            column_names=columns,
        )

        max_changed = max(row["changed"] for row in rows)

        if max_changed > last_changed:
            _insert_watermark(ch_client, table_name, max_changed)
            print(
                f"Loaded {len(rows)} rows into {raw_table}. "
                f"Watermark updated to {max_changed:%Y-%m-%d %H:%M:%S}"
            )
        else:
            print(
                f"Loaded {len(rows)} overlap rows into {raw_table}. "
                f"Watermark not changed: {last_changed:%Y-%m-%d %H:%M:%S}"
            )

    finally:
        mysql.close()

    print(f"=== END incremental load: {table_name} ===")


default_args = {
    "owner": "serzhan",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="mysql_to_clickhouse_incremental_all_tables",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["incremental", "mysql", "clickhouse", "raw"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup(group_id="incremental_raw_loads") as incremental_raw_loads:
        for cfg in TABLES:
            PythonOperator(
                task_id=f"load_{cfg['table_name']}",
                python_callable=incremental_load_table,
                op_kwargs={
                    "table_name": cfg["table_name"],
                    "raw_table": cfg["raw_table"],
                },
            )

    start >> incremental_raw_loads >> end
