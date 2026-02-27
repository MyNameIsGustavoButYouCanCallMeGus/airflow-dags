from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import pymysql
import clickhouse_connect


def list_mysql_tables():
    conn = BaseHook.get_connection("tourservice_mysql")

    connection = pymysql.connect(
        host=conn.host,
        port=int(conn.port),
        user=conn.login,
        password=conn.password,
        database=conn.schema,
        connect_timeout=10,
        charset="utf8"
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            rows = cursor.fetchall()

        tables = [r[0] for r in rows]
        print(f"MySQL tables in schema '{conn.schema}': {len(tables)}")
        for t in tables:
            print(" -", t)

    finally:
        connection.close()


def list_clickhouse_tables():
    conn = BaseHook.get_connection("clickhouse")

    # Используем коннект из Airflow (если ты там хранишь host/port/login/password/schema)
    ch_host = conn.host
    ch_port = int(conn.port) if conn.port else 8123
    ch_user = conn.login or "default"
    ch_pass = conn.password or ""
    ch_db = conn.schema or "default"

    client = clickhouse_connect.get_client(
        host=ch_host,
        port=ch_port,
        username=ch_user,
        password=ch_pass,
        interface="http",
        database=ch_db,
    )

    # В ClickHouse "SHOW TABLES" зависит от текущей базы, поэтому надежнее через system.tables
    q = """
        SELECT name
        FROM system.tables
        WHERE database = %(db)s
        ORDER BY name
    """
    result = client.query(q, parameters={"db": ch_db})
    tables = [r[0] for r in result.result_rows]

    print(f"ClickHouse tables in database '{ch_db}': {len(tables)}")
    for t in tables:
        print(" -", t)


with DAG(
    dag_id="list_tables_mysql_clickhouse_serzhan",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["introspect", "mysql", "clickhouse"]
) as dag:

    mysql_tables_task = PythonOperator(
        task_id="list_mysql_tables",
        python_callable=list_mysql_tables
    )

    ch_tables_task = PythonOperator(
        task_id="list_clickhouse_tables",
        python_callable=list_clickhouse_tables
    )

    mysql_tables_task >> ch_tables_task
