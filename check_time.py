from datetime import datetime

import pymysql
import clickhouse_connect

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator


MYSQL_CONN_ID = "tourservice_mysql"
CH_CONN_ID = "clickhouse"


def check_timezones():

    print("\n========== AIRFLOW / PYTHON ==========")

    import datetime as dt
    import time
    import os

    print("Python datetime.now():", dt.datetime.now())
    print("Python datetime.utcnow():", dt.datetime.utcnow())
    print("time.tzname:", time.tzname)
    print("TZ env:", os.environ.get("TZ"))

    print("\n========== MYSQL ==========")

    mysql_conn = BaseHook.get_connection(MYSQL_CONN_ID)

    mysql = pymysql.connect(
        host=mysql_conn.host,
        port=int(mysql_conn.port),
        user=mysql_conn.login,
        password=mysql_conn.password,
        database=mysql_conn.schema,
        charset="utf8",
        cursorclass=pymysql.cursors.DictCursor,
    )

    with mysql.cursor() as cursor:

        cursor.execute("SELECT NOW() as now_mysql")
        print("MySQL NOW():", cursor.fetchone())

        cursor.execute("SELECT UTC_TIMESTAMP() as utc_mysql")
        print("MySQL UTC_TIMESTAMP():", cursor.fetchone())

        cursor.execute("SELECT @@global.time_zone as global_tz")
        print("MySQL global timezone:", cursor.fetchone())

        cursor.execute("SELECT @@session.time_zone as session_tz")
        print("MySQL session timezone:", cursor.fetchone())
        
        cursor.execute("""
            select * from dict32 where changed = (
            SELECT MAX(changed) AS max_changed_dict32
            FROM dict32)
        """)
        print("MySQL dict32 MAX(changed):", cursor.fetchone())

    mysql.close()

    print("\n========== CLICKHOUSE ==========")

    ch_conn = BaseHook.get_connection(CH_CONN_ID)

    ch = clickhouse_connect.get_client(
        host=ch_conn.host,
        port=int(ch_conn.port) if ch_conn.port else 8123,
        username=ch_conn.login or "default",
        password=ch_conn.password or "",
        database=ch_conn.schema or "default",
    )

    res = ch.query("""
    SELECT
        now() as ch_now,
        now64() as ch_now64,
        timezone(),
        serverTimeZone()
    """)

    print("ClickHouse:", res.result_rows)

    res = ch.query("""
    SELECT
        toTimeZone(now(), 'UTC') as ch_utc,
        toTimeZone(now(), 'Asia/Almaty') as ch_almaty
    """)

    print("ClickHouse timezone conversions:", res.result_rows)

    print("\n========== COMPARISON ==========")

    res = ch.query("""
    SELECT
        now() as ch_now,
        toTimeZone(now(), 'UTC') as ch_utc
    """)

    ch_now, ch_utc = res.result_rows[0]

    print("ClickHouse now:", ch_now)
    print("ClickHouse UTC:", ch_utc)

    print("\n========== DONE ==========")


default_args = {
    "owner": "serzhan",
}

with DAG(
    dag_id="debug_timezones_all_systems",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["debug", "timezone"],
) as dag:

    check = PythonOperator(
        task_id="check_timezones",
        python_callable=check_timezones,
    )
