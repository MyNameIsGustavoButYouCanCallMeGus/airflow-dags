from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import pymysql
import json


MYSQL_CONN_ID = "tourservice_mysql"
MYSQL_SCHEMA = None


def extract_mysql_schema():
    conn = BaseHook.get_connection(MYSQL_CONN_ID)
    schema = MYSQL_SCHEMA or conn.schema

    print(f"=== EXTRACTING SCHEMA FOR DATABASE: {schema} ===")

    connection = pymysql.connect(
        host=conn.host,
        port=int(conn.port),
        user=conn.login,
        password=conn.password,
        database=schema,
        charset="utf8",
        cursorclass=pymysql.cursors.DictCursor
    )

    schema_result = {}

    try:
        with connection.cursor() as cur:

            # Получаем список таблиц
            cur.execute(f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
            """)
            tables = [row["table_name"] for row in cur.fetchall()]

            print(f"Found {len(tables)} tables")

            for table in tables:
                print(f"\n--- TABLE: {table} ---")

                cur.execute(f"""
                    SELECT
                        column_name,
                        column_type,
                        is_nullable,
                        column_default,
                        column_key,
                        extra
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}'
                      AND table_name = '{table}'
                    ORDER BY ordinal_position
                """)

                columns = cur.fetchall()
                schema_result[table] = columns

                for col in columns:
                    print(
                        f"{col['column_name']:25} | "
                        f"{col['column_type']:20} | "
                        f"nullable={col['is_nullable']:3} | "
                        f"default={col['column_default']} | "
                        f"key={col['column_key']} | "
                        f"extra={col['extra']}"
                    )

    finally:
        connection.close()

    print("\n=== DONE ===")

    # Можно вернуть JSON, чтобы видеть в XCom
    return schema_result


with DAG(
    dag_id="extract_mysql_schema_www_serzhan",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mysql", "schema", "introspection"]
) as dag:

    task = PythonOperator(
        task_id="extract_schema",
        python_callable=extract_mysql_schema
    )
