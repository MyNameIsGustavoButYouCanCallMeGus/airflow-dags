from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import time
import pymysql
import clickhouse_connect


MYSQL_CONN_ID = "tourservice_mysql"
CH_CONN_ID = "clickhouse"

MYSQL_SCHEMA = None   # если None -> берем conn.schema (у тебя это 'www')
CH_DB = None          # если None -> берем conn.schema (у тебя это 'fondkamkor')

BATCH_SIZE = 10_000


def _mysql_conn():
    conn = BaseHook.get_connection(MYSQL_CONN_ID)
    schema = MYSQL_SCHEMA or conn.schema

    connection = pymysql.connect(
        host=conn.host,
        port=int(conn.port),
        user=conn.login,
        password=conn.password,
        database=schema,
        connect_timeout=20,
        charset="utf8",
        cursorclass=pymysql.cursors.SSCursor  # server-side cursor, чтобы не тащить всё в память
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
        interface="http",
        database=db,
    )
    return db, client


def _quote_ident(name: str) -> str:
    # для ClickHouse бэктики ок
    return f"`{name}`"


def sync_table_full(table: str):
    mysql_schema, mysql_connection = _mysql_conn()
    ch_db, ch = _ch_client()

    mysql_fq = f"`{mysql_schema}`.`{table}`"
    ch_fq = f"`{ch_db}`.`{table}`"

    print(f"=== FULL RELOAD {mysql_fq} -> {ch_fq} | batch={BATCH_SIZE} ===")

    # 1) TRUNCATE
    t0 = time.time()
    ch.command(f"TRUNCATE TABLE {ch_fq}")
    print(f"TRUNCATE OK in {time.time()-t0:.2f}s")

    total = 0
    t_start = time.time()

    try:
        with mysql_connection.cursor() as cur:
            cur.execute(f"SELECT * FROM {mysql_fq}")

            col_names = [d[0] for d in cur.description]
            print(f"MySQL columns ({len(col_names)}): {col_names}")

            batch = []
            batch_rows = 0

            while True:
                row = cur.fetchone()
                if row is None:
                    break

                batch.append(row)
                batch_rows += 1

                if batch_rows >= BATCH_SIZE:
                    ch.insert(
                        table=f"{ch_db}.{table}",
                        data=batch,
                        column_names=col_names,
                    )
                    total += batch_rows
                    elapsed = time.time() - t_start
                    rps = total / elapsed if elapsed > 0 else 0
                    print(f"Inserted {total} rows | elapsed={elapsed:.2f}s | ~{rps:.0f} rows/s")
                    batch = []
                    batch_rows = 0

            if batch_rows > 0:
                ch.insert(
                    table=f"{ch_db}.{table}",
                    data=batch,
                    column_names=col_names,
                )
                total += batch_rows

    finally:
        mysql_connection.close()

    elapsed = time.time() - t_start
    rps = total / elapsed if elapsed > 0 else 0

    ch_count = ch.query(f"SELECT count() FROM {ch_fq}").result_rows[0][0]

    print(
        f"=== DONE {table} ===\n"
        f"MySQL read rows: {total}\n"
        f"ClickHouse count: {ch_count}\n"
        f"Time: {elapsed:.2f}s\n"
        f"Speed: ~{rps:.0f} rows/s\n"
    )


def sync_dict13():
    return sync_table_full("dict13")


def sync_dict14():
    return sync_table_full("dict14")


def sync_dict15():
    return sync_table_full("dict15")


def sync_dict59():
    return sync_table_full("dict59")


with DAG(
    dag_id="sync_mysql_to_clickhouse_dicts_serzhan2",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sync", "mysql", "clickhouse", "dict"]
) as dag:

    t13 = PythonOperator(task_id="sync_dict13", python_callable=sync_dict13)
    t14 = PythonOperator(task_id="sync_dict14", python_callable=sync_dict14)
    t15 = PythonOperator(task_id="sync_dict15", python_callable=sync_dict15)
    t59 = PythonOperator(task_id="sync_dict59", python_callable=sync_dict59)

    # можно параллелить (без зависимостей)
    # или цепочкой, чтобы нагрузку контролировать:
    t13 >> t14 >> t15 >> t59
