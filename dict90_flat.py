from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import time
import pymysql
import clickhouse_connect


MYSQL_CONN_ID = "tourservice_mysql"
CH_CONN_ID = "clickhouse"

MYSQL_SCHEMA = None
CH_DB = None

# Tunable
BATCH_SIZE = 200_000


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
        cursorclass=pymysql.cursors.SSCursor,
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
        interface="http",
        database=db,
        compress=True,
    )
    return db, client


def load_dict90():
    mysql_schema, mysql_connection = _mysql_conn()
    ch_db, ch = _ch_client()

    target_table = "dict90"
    ch_fq = f"`{ch_db}`.`{target_table}`"
    print(f"=== LOAD {mysql_schema}.dict90 -> {ch_fq} | batch={BATCH_SIZE} ===")

    t0 = time.time()
    ch.command(f"TRUNCATE TABLE {ch_fq}")
    print(f"TRUNCATE OK in {time.time()-t0:.2f}s")

    src_sql = f"""
    select
        d.rid             as rid,
        d.number          as number,
        d.country1_id     as country1_id,
        d.country2_id     as country2_id,
        d.country3_id     as country3_id,
        d.country4_id     as country4_id,
        d.country5_id     as country5_id,
        d.country6_id     as country6_id,
        d.currency        as currency,
        d.date_start      as date_start,
        d.date_end        as date_end,
        d.touragent_bin   as touragent_bin,
        d.airport_start   as airport_start,
        d.airport_end     as airport_end,
        d.flight_start    as flight_start,
        d.flight_end      as flight_end,
        d.airlines        as airlines,
        d.from_cabinet    as from_cabinet,
        d.passport        as passport,
        d.tid             as tid,
        d.qid             as qid,
        d.created         as created,
        d.changed         as changed,
        d.user            as user,
        d.enabled         as enabled
    from `{mysql_schema}`.`dict90` d
    """

    col_names = [
        "rid",
        "number",
        "country1_id",
        "country2_id",
        "country3_id",
        "country4_id",
        "country5_id",
        "country6_id",
        "currency",
        "date_start",
        "date_end",
        "touragent_bin",
        "airport_start",
        "airport_end",
        "flight_start",
        "flight_end",
        "airlines",
        "from_cabinet",
        "passport",
        "tid",
        "qid",
        "created",
        "changed",
        "user",
        "enabled",
    ]

    total = 0
    t_start = time.time()

    try:
        with mysql_connection.cursor() as cur:
            cur.execute(src_sql)

            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break

                ch.insert(
                    table=f"{ch_db}.{target_table}",
                    data=rows,
                    column_names=col_names,
                )
                total += len(rows)

                elapsed = time.time() - t_start
                rps = total / elapsed if elapsed > 0 else 0
                print(f"Inserted {total} rows | elapsed={elapsed:.1f}s | ~{rps:.0f} rows/s")

    finally:
        mysql_connection.close()

    elapsed = time.time() - t_start
    rps = total / elapsed if elapsed > 0 else 0
    ch_count = ch.query(f"select count() from {ch_fq}").result_rows[0][0]

    print(
        f"=== DONE dict90 ===\n"
        f"MySQL read rows: {total}\n"
        f"ClickHouse count: {ch_count}\n"
        f"Time: {elapsed:.2f}s\n"
        f"Speed: ~{rps:.0f} rows/s\n"
    )


def load_dict91():
    mysql_schema, mysql_connection = _mysql_conn()
    ch_db, ch = _ch_client()

    target_table = "dict91"
    ch_fq = f"`{ch_db}`.`{target_table}`"
    print(f"=== LOAD {mysql_schema}.dict91 -> {ch_fq} | batch={BATCH_SIZE} ===")

    t0 = time.time()
    ch.command(f"TRUNCATE TABLE {ch_fq}")
    print(f"TRUNCATE OK in {time.time()-t0:.2f}s")

    # IMPORTANT:
    # This assumes CH table fondkamkor.dict91 has columns:
    # rid, bindrid, sub_date_start, sub_date_end, sub_airport, sub_airlines, sub_flight, changed, user, enabled
    # If your CH dict91 has different column names, adjust col_names + select aliases accordingly.
    src_sql = f"""
    select
        d2.rid            as rid,
        d2.bindrid        as bindrid,
        d2.sub_date_start as sub_date_start,
        d2.sub_date_end   as sub_date_end,
        d2.sub_airport    as sub_airport,
        d2.sub_airlines   as sub_airlines,
        d2.sub_flight     as sub_flight,
        d2.changed        as changed,
        d2.user           as user,
        d2.enabled        as enabled
    from `{mysql_schema}`.`dict91` d2
    """

    col_names = [
        "rid",
        "bindrid",
        "sub_date_start",
        "sub_date_end",
        "sub_airport",
        "sub_airlines",
        "sub_flight",
        "changed",
        "user",
        "enabled",
    ]

    total = 0
    t_start = time.time()

    try:
        with mysql_connection.cursor() as cur:
            cur.execute(src_sql)

            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break

                ch.insert(
                    table=f"{ch_db}.{target_table}",
                    data=rows,
                    column_names=col_names,
                )
                total += len(rows)

                elapsed = time.time() - t_start
                rps = total / elapsed if elapsed > 0 else 0
                print(f"Inserted {total} rows | elapsed={elapsed:.1f}s | ~{rps:.0f} rows/s")

    finally:
        mysql_connection.close()

    elapsed = time.time() - t_start
    rps = total / elapsed if elapsed > 0 else 0
    ch_count = ch.query(f"select count() from {ch_fq}").result_rows[0][0]

    print(
        f"=== DONE dict91 ===\n"
        f"MySQL read rows: {total}\n"
        f"ClickHouse count: {ch_count}\n"
        f"Time: {elapsed:.2f}s\n"
        f"Speed: ~{rps:.0f} rows/s\n"
    )


def build_dict90_flat_in_clickhouse():
    ch_db, ch = _ch_client()

    flat = f"`{ch_db}`.`dict90_flat`"
    d90 = f"`{ch_db}`.`dict90`"
    d91 = f"`{ch_db}`.`dict91`"

    print(f"=== BUILD dict90_flat inside ClickHouse: {d90} LEFT JOIN {d91} -> {flat} ===")

    t0 = time.time()
    ch.command(f"TRUNCATE TABLE {flat}")
    print(f"TRUNCATE OK in {time.time()-t0:.2f}s")

    t_start = time.time()

    ch.command(f"""
    INSERT INTO {flat}
    SELECT
        d.rid             as rid,
        d.number          as number,
        d.country1_id     as country1_id,
        d.country2_id     as country2_id,
        d.country3_id     as country3_id,
        d.country4_id     as country4_id,
        d.country5_id     as country5_id,
        d.country6_id     as country6_id,
        d.currency        as currency,
        d.date_start      as date_start,
        d.date_end        as date_end,
        d.touragent_bin   as touragent_bin,
        d.airport_start   as airport_start,
        d.airport_end     as airport_end,
        d.flight_start    as flight_start,
        d.flight_end      as flight_end,
        d.airlines        as airlines,
        d.from_cabinet    as from_cabinet,
        d.passport        as passport,
        d.tid             as tid,
        d.qid             as qid,
        d.created         as created,
        d.changed         as changed,
        d.user            as user,
        d.enabled         as enabled,
        ifNull(d2.rid, 0) as sub_rid,
        d2.bindrid        as sub_bindrid,
        d2.sub_date_start as sub_date_start,
        d2.sub_date_end   as sub_date_end,
        d2.sub_airport    as sub_airport,
        d2.sub_airlines   as sub_airlines,
        d2.sub_flight     as sub_flight,
        d2.changed        as sub_changed,
        d2.user           as sub_user,
        d2.enabled        as sub_enabled
    FROM {d90} d
    LEFT JOIN {d91} d2
        ON d.rid = d2.bindrid
    """)

    elapsed = time.time() - t_start
    ch_count = ch.query(f"select count() from {flat}").result_rows[0][0]
    print(f"=== DONE build dict90_flat ===\nClickHouse count: {ch_count}\nTime: {elapsed:.2f}s\n")


with DAG(
    dag_id="sync_mysql_to_clickhouse_dict90_flat_stage_then_build_serzhan",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sync", "mysql", "clickhouse", "flat"]
) as dag:

    t1 = PythonOperator(
        task_id="load_dict90",
        python_callable=load_dict90
    )

    t2 = PythonOperator(
        task_id="load_dict91",
        python_callable=load_dict91
    )

    t3 = PythonOperator(
        task_id="build_dict90_flat",
        python_callable=build_dict90_flat_in_clickhouse
    )

    t1 >> t2 >> t3
