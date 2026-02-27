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

BATCH_SIZE = 50_000   # для 5 млн лучше 50k (можешь вернуть 10k если хочешь)


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
        charset="utf8",
        cursorclass=pymysql.cursors.SSCursor  # streaming
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


def load_dict90_flat():
    mysql_schema, mysql_connection = _mysql_conn()
    ch_db, ch = _ch_client()

    target_table = "dict90_flat"
    ch_fq = f"`{ch_db}`.`{target_table}`"
    print(f"=== LOAD FLAT {mysql_schema}.dict90 + {mysql_schema}.dict91 -> {ch_fq} | batch={BATCH_SIZE} ===")

    # 1) TRUNCATE target
    t0 = time.time()
    ch.command(f"TRUNCATE TABLE {ch_fq}")
    print(f"TRUNCATE OK in {time.time()-t0:.2f}s")

    # 2) Source JOIN query from MySQL (schema www)
    src_sql = f"""
    SELECT
        d.rid              AS rid,
        d.number           AS number,
        d.country1_id      AS country1_id,
        d.country2_id      AS country2_id,
        d.country3_id      AS country3_id,
        d.country4_id      AS country4_id,
        d.country5_id      AS country5_id,
        d.country6_id      AS country6_id,
        d.currency         AS currency,
        d.date_start       AS date_start,
        d.date_end         AS date_end,
        d.touragent_bin    AS touragent_bin,
        d.airport_start    AS airport_start,
        d.airport_end      AS airport_end,
        d.flight_start     AS flight_start,
        d.flight_end       AS flight_end,
        d.airlines         AS airlines,
        d.from_cabinet     AS from_cabinet,
        d.passport         AS passport,
        d.tid              AS tid,
        d.qid              AS qid,
        d.created          AS created,
        d.changed          AS changed,
        d.user             AS user,
        d.enabled          AS enabled,

        COALESCE(d2.rid, 0)        AS sub_rid,       -- важно: sub_rid NOT NULL в CH
        d2.bindrid                 AS sub_bindrid,
        d2.sub_date_start          AS sub_date_start,
        d2.sub_date_end            AS sub_date_end,
        d2.sub_airport             AS sub_airport,
        d2.sub_airlines            AS sub_airlines,
        d2.sub_flight              AS sub_flight,
        d2.changed                 AS sub_changed,
        d2.user                    AS sub_user,
        d2.enabled                 AS sub_enabled
    FROM `{mysql_schema}`.`dict90` d
    LEFT JOIN `{mysql_schema}`.`dict91` d2
        ON d.rid = d2.bindrid
    """

    # порядок колонок должен соответствовать SELECT и таблице CH
    col_names = [
        "rid",
        "number",
        "country1_id", "country2_id", "country3_id", "country4_id", "country5_id", "country6_id",
        "currency",
        "date_start", "date_end",
        "touragent_bin",
        "airport_start", "airport_end",
        "flight_start", "flight_end",
        "airlines",
        "from_cabinet",
        "passport",
        "tid",
        "qid",
        "created",
        "changed",
        "user",
        "enabled",
        "sub_rid",
        "sub_bindrid",
        "sub_date_start",
        "sub_date_end",
        "sub_airport",
        "sub_airlines",
        "sub_flight",
        "sub_changed",
        "sub_user",
        "sub_enabled",
    ]

    total = 0
    t_start = time.time()

    try:
        with mysql_connection.cursor() as cur:
            cur.execute(src_sql)

            batch = []
            while True:
                row = cur.fetchone()
                if row is None:
                    break

                batch.append(row)

                if len(batch) >= BATCH_SIZE:
                    ch.insert(
                        table=f"{ch_db}.{target_table}",
                        data=batch,
                        column_names=col_names,
                    )
                    total += len(batch)
                    elapsed = time.time() - t_start
                    rps = total / elapsed if elapsed > 0 else 0
                    print(f"Inserted {total} rows | elapsed={elapsed:.1f}s | ~{rps:.0f} rows/s")
                    batch = []

            if batch:
                ch.insert(
                    table=f"{ch_db}.{target_table}",
                    data=batch,
                    column_names=col_names,
                )
                total += len(batch)

    finally:
        mysql_connection.close()

    elapsed = time.time() - t_start
    rps = total / elapsed if elapsed > 0 else 0
    ch_count = ch.query(f"SELECT count() FROM {ch_fq}").result_rows[0][0]

    print(
        f"=== DONE dict90_flat ===\n"
        f"MySQL read rows: {total}\n"
        f"ClickHouse count: {ch_count}\n"
        f"Time: {elapsed:.2f}s\n"
        f"Speed: ~{rps:.0f} rows/s\n"
    )


with DAG(
    dag_id="sync_mysql_to_clickhouse_dict90_flat_serzhan",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sync", "mysql", "clickhouse", "flat"]
) as dag:

    load_flat = PythonOperator(
        task_id="load_dict90_flat",
        python_callable=load_dict90_flat
    )
