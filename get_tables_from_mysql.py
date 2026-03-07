from __future__ import annotations

from datetime import datetime, timedelta

import pymysql

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator


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


def print_mysql_schema():
    schema, mysql_connection = _mysql_conn()

    print(f"\n=== MYSQL SCHEMA: {schema} ===\n")

    try:
        with mysql_connection.cursor() as cur:

            # получаем все таблицы
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                ORDER BY table_name
                """,
                (schema,),
            )

            tables = [r["table_name"] for r in cur.fetchall()]

            print(f"Found {len(tables)} tables\n")

            for table in tables:

                print(f"\n==============================")
                print(f"TABLE: {table}")
                print(f"==============================")

                cur.execute(
                    """
                    SELECT
                        ordinal_position,
                        column_name,
                        column_type,
                        is_nullable,
                        column_key,
                        extra
                    FROM information_schema.columns
                    WHERE table_schema = %s
                    AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (schema, table),
                )

                columns = cur.fetchall()

                for c in columns:
                    print(
                        f"{c['ordinal_position']:>3}. "
                        f"{c['column_name']} | "
                        f"{c['column_type']} | "
                        f"nullable={c['is_nullable']} | "
                        f"key={c['column_key']} | "
                        f"{c['extra']}"
                    )

    finally:
        mysql_connection.close()


default_args = {
    "owner": "serzhan",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="mysql_print_schema_serzhan",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["mysql", "schema", "debug"],
) as dag:

    t = PythonOperator(
        task_id="print_mysql_schema",
        python_callable=print_mysql_schema,
    )
