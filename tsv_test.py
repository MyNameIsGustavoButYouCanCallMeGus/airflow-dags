from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import time
import pymysql
import clickhouse_connect


MYSQL_CONN_ID = "tourservice_mysql"
CH_CONN_ID = "clickhouse"

MYSQL_SCHEMA = None   # None -> возьмет conn.schema (www)
CH_DB = None          # None -> возьмет conn.schema (fondkamkor)

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
        read_timeout=600,
        write_timeout=600,
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


def _to_dt(x):
    """Минимальный конвертер: datetime/str/bytes -> datetime или None. Zero-date -> None."""
    if x is None:
        return None

    if isinstance(x, datetime):
        return x

    if isinstance(x, (bytes, bytearray)):
        x = x.decode("utf-8", errors="ignore")

    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        if s.startswith("0000-00-00"):
            return None

        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(s[:26], fmt)
            except ValueError:
                pass

        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                return datetime.strptime(s[:26], fmt)
            except ValueError:
                pass

        # если вообще непонятно — лучше не падать
        return None

    return None


def load_dict31():
    mysql_schema, mysql_connection = _mysql_conn()
    ch_db, ch = _ch_client()

    print(f"=== LOAD {mysql_schema}.dict31 -> `{ch_db}`.`dict31` ===")
    ch.command(f"TRUNCATE TABLE `{ch_db}`.`dict31`")

    src_sql = f"""
    select
        rid, changed, user, enabled, name, type, currency, operatorid,
        bik, bank, about, filialid, balance, transactions, bin
    from `{mysql_schema}`.`dict31`
    """

    col_names = [
        "rid", "changed", "user", "enabled", "name", "type", "currency", "operatorid",
        "bik", "bank", "about", "filialid", "balance", "transactions", "bin",
    ]
    idx_changed = col_names.index("changed")

    total = 0
    t0 = time.time()

    try:
        with mysql_connection.cursor() as cur:
            cur.execute(src_sql)
            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break

                fixed = []
                for r in rows:
                    rr = list(r)
                    rr[idx_changed] = _to_dt(rr[idx_changed])
                    fixed.append(tuple(rr))

                ch.insert(f"{ch_db}.dict31", fixed, column_names=col_names)
                total += len(fixed)

                elapsed = time.time() - t0
                print(f"dict31 inserted={total} | {elapsed:.1f}s | ~{total/elapsed:.0f} r/s")

    finally:
        mysql_connection.close()

    print(f"=== DONE dict31 rows={total} ===")


def load_dict32():
    mysql_schema, mysql_connection = _mysql_conn()
    ch_db, ch = _ch_client()

    print(f"=== LOAD {mysql_schema}.dict32 -> `{ch_db}`.`dict32` ===")
    ch.command(f"TRUNCATE TABLE `{ch_db}`.`dict32`")

    src_sql = f"""
    select
        rid, changed, user, enabled, bindrid, money, mode, qid, docid,
        doctemplateid, userid, datetime, first_datetime, msg
    from `{mysql_schema}`.`dict32`
    """

    col_names = [
        "rid", "changed", "user", "enabled", "bindrid", "money", "mode", "qid", "docid",
        "doctemplateid", "userid", "datetime", "first_datetime", "msg",
    ]
    idx_changed = col_names.index("changed")
    idx_dt = col_names.index("datetime")
    idx_first = col_names.index("first_datetime")

    total = 0
    t0 = time.time()

    try:
        with mysql_connection.cursor() as cur:
            cur.execute(src_sql)
            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break

                fixed = []
                for r in rows:
                    rr = list(r)
                    rr[idx_changed] = _to_dt(rr[idx_changed])
                    rr[idx_dt] = _to_dt(rr[idx_dt])
                    rr[idx_first] = _to_dt(rr[idx_first])
                    fixed.append(tuple(rr))

                ch.insert(f"{ch_db}.dict32", fixed, column_names=col_names)
                total += len(fixed)

                elapsed = time.time() - t0
                print(f"dict32 inserted={total} | {elapsed:.1f}s | ~{total/elapsed:.0f} r/s")

    finally:
        mysql_connection.close()

    print(f"=== DONE dict32 rows={total} ===")


def build_dict31_flat():
    ch_db, ch = _ch_client()
    print(f"=== BUILD `{ch_db}`.`dict31_flat` from `{ch_db}`.`dict31` + `{ch_db}`.`dict32` ===")

    ch.command(f"TRUNCATE TABLE `{ch_db}`.`dict31_flat`")

    sql = f"""
    INSERT INTO `{ch_db}`.`dict31_flat`
    SELECT
        t.rid               as d31_rid,
        t.changed           as d31_changed,
        t.user              as d31_user,
        t.enabled           as d31_enabled,
        t.name              as d31_name,
        t.type              as d31_type,
        t.currency          as d31_currency,
        t.operatorid        as d31_operatorid,
        t.bik               as d31_bik,
        t.bank              as d31_bank,
        t.about             as d31_about,
        t.filialid          as d31_filialid,
        t.balance           as d31_balance,
        t.transactions      as d31_transactions,
        t.bin               as d31_bin,
        t2.rid              as d32_rid,
        t2.changed          as d32_changed,
        t2.user             as d32_user,
        t2.enabled          as d32_enabled,
        t2.bindrid          as d32_bindrid,
        t2.money            as d32_money,
        t2.mode             as d32_mode,
        t2.qid              as d32_qid,
        t2.docid            as d32_docid,
        t2.doctemplateid    as d32_doctemplateid,
        t2.userid           as d32_userid,
        t2.datetime         as d32_datetime,
        t2.first_datetime   as d32_first_datetime,
        t2.msg              as d32_msg
    FROM `{ch_db}`.`dict31` t
    LEFT JOIN `{ch_db}`.`dict32` t2
        ON t.rid = t2.bindrid
    """

    t0 = time.time()
    ch.command(sql)
    cnt = ch.query(f"SELECT count() FROM `{ch_db}`.`dict31_flat`").result_rows[0][0]
    print(f"=== DONE BUILD flat | rows={cnt} | {time.time()-t0:.2f}s ===")


with DAG(
    dag_id="sync_mysql_to_clickhouse_dict31_32_then_join_serzhan",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sync", "mysql", "clickhouse", "dict31", "dict32", "flat"],
) as dag:

    t1 = PythonOperator(task_id="load_dict31", python_callable=load_dict31)
    t2 = PythonOperator(task_id="load_dict32", python_callable=load_dict32)
    t3 = PythonOperator(task_id="build_dict31_flat", python_callable=build_dict31_flat)

    t1 >> t2 >> t3
