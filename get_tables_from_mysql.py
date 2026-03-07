from __future__ import annotations

import csv
import os
from datetime import datetime, timedelta

import pymysql

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator


MYSQL_CONN_ID = "tourservice_mysql"
MYSQL_SCHEMA = None  # None -> возьмет conn.schema
OUTPUT_DIR = "/tmp/mysql_schema_export"


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


def export_mysql_schema():
    schema, mysql_connection = _mysql_conn()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    tables_csv = os.path.join(OUTPUT_DIR, f"{schema}_tables.csv")
    columns_csv = os.path.join(OUTPUT_DIR, f"{schema}_columns.csv")

    print(f"=== EXPORT MYSQL METADATA FOR SCHEMA `{schema}` ===")

    try:
        with mysql_connection.cursor() as cur:
            # 1. Все таблицы
            cur.execute(
                """
                SELECT
                    table_schema,
                    table_name,
                    table_type,
                    engine,
                    table_rows,
                    create_time,
                    update_time
                FROM information_schema.tables
                WHERE table_schema = %s
                ORDER BY table_name
                """,
                (schema,),
            )
            tables = cur.fetchall()

            print(f"Found {len(tables)} tables in schema `{schema}`")
            for t in tables:
                print(
                    f"[TABLE] {t['table_name']} | "
                    f"type={t['table_type']} | "
                    f"engine={t['engine']} | "
                    f"rows={t['table_rows']}"
                )

            with open(tables_csv, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "table_schema",
                        "table_name",
                        "table_type",
                        "engine",
                        "table_rows",
                        "create_time",
                        "update_time",
                    ],
                )
                writer.writeheader()
                writer.writerows(tables)

            # 2. Все колонки по всем таблицам
            cur.execute(
                """
                SELECT
                    table_schema,
                    table_name,
                    ordinal_position,
                    column_name,
                    column_type,
                    data_type,
                    is_nullable,
                    column_default,
                    column_key,
                    extra,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    datetime_precision
                FROM information_schema.columns
                WHERE table_schema = %s
                ORDER BY table_name, ordinal_position
                """,
                (schema,),
            )
            columns = cur.fetchall()

            print(f"Found {len(columns)} columns in schema `{schema}`")

            current_table = None
            for c in columns:
                if c["table_name"] != current_table:
                    current_table = c["table_name"]
                    print(f"\n=== {current_table} ===")

                print(
                    f"{c['ordinal_position']:>3}. "
                    f"{c['column_name']} | "
                    f"column_type={c['column_type']} | "
                    f"data_type={c['data_type']} | "
                    f"nullable={c['is_nullable']} | "
                    f"key={c['column_key']} | "
                    f"default={c['column_default']} | "
                    f"extra={c['extra']}"
                )

            with open(columns_csv, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "table_schema",
                        "table_name",
                        "ordinal_position",
                        "column_name",
                        "column_type",
                        "data_type",
                        "is_nullable",
                        "column_default",
                        "column_key",
                        "extra",
                        "character_maximum_length",
                        "numeric_precision",
                        "numeric_scale",
                        "datetime_precision",
                    ],
                )
                writer.writeheader()
                writer.writerows(columns)

        print("\n=== DONE ===")
        print(f"Tables CSV:  {tables_csv}")
        print(f"Columns CSV: {columns_csv}")

    finally:
        mysql_connection.close()


default_args = {
    "owner": "serzhan",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=20),
}

with DAG(
    dag_id="mysql_schema_export_serzhan",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # вручную
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["mysql", "schema", "metadata", "debug"],
) as dag:

    export_schema = PythonOperator(
        task_id="export_mysql_schema",
        python_callable=export_mysql_schema,
    )
