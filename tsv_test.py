from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, date, time as dtime
import time
import pymysql
import clickhouse_connect
from collections import defaultdict, deque


MYSQL_CONN_ID = "tourservice_mysql"
CH_CONN_ID = "clickhouse"

MYSQL_SCHEMA = None
CH_DB = None

BATCH_SIZE = 200_000
ZERO_DATE_STRINGS = {"0000-00-00", "0000-00-00 00:00:00", "0000-00-00 00:00:00.000000"}


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


def _decode_if_bytes(x):
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", errors="ignore")
    return x


def _make_dt_converters():
    # stats per task call
    zero_by_col = defaultdict(int)
    parse_fail_by_col = defaultdict(int)
    type_fail_by_col = defaultdict(int)
    samples_by_col = defaultdict(lambda: deque(maxlen=5))

    def _mark_sample(col, value):
        try:
            samples_by_col[col].append(value if isinstance(value, str) else repr(value))
        except Exception:
            samples_by_col[col].append("<sample_error>")

    def _to_dt(x, col):
        if x is None:
            return None
        x = _decode_if_bytes(x)

        if isinstance(x, datetime):
            return x
        if isinstance(x, date):
            return datetime.combine(x, dtime.min)

        if isinstance(x, str):
            s = x.strip()
            if not s:
                return None
            if s in ZERO_DATE_STRINGS or s.startswith("0000-00-00"):
                zero_by_col[col] += 1
                _mark_sample(col, s)
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

            parse_fail_by_col[col] += 1
            _mark_sample(col, s)
            return None

        type_fail_by_col[col] += 1
        _mark_sample(col, x)
        return None

    def _print_stats(prefix=""):
        def _fmt(d):
            return "{ " + ", ".join(f"{k}: {v}" for k, v in sorted(d.items())) + " }"
        print(prefix + f"Zero-date by column: {_fmt(zero_by_col)}")
        print(prefix + f"Parse-fail by column: {_fmt(parse_fail_by_col)}")
        print(prefix + f"Type-fail by column: {_fmt(type_fail_by_col)}")
        if sum(zero_by_col.values()) or sum(parse_fail_by_col.values()) or sum(type_fail_by_col.values()):
            for col in sorted(samples_by_col.keys()):
                if samples_by_col[col]:
                    print(prefix + f"Samples[{col}]: {list(samples_by_col[col])}")

    return _to_dt, _print_stats


def load_dict31():
    to_dt, print_stats = _make_dt_converters()

    mysql_schema, mysql_connection = _mysql_conn()
    ch_db, ch = _ch_client()

    target_table = "dict31"
    ch_fq = f"`{ch_db}`.`{target_table}`"

    print(f"=== LOAD {mysql_schema}.dict31 -> {ch_fq} | batch={BATCH_SIZE} ===")

    ch.command(f"TRUNCATE TABLE {ch_fq}")

    src_sql = f"""
    select
        rid,
        changed,
        user,
        enabled,
        name,
        type,
        currency,
        operatorid,
        bik,
        bank,
        about,
        filialid,
        balance,
        transactions,
        bin
    from `{mysql_schema}`.`dict31`
    """

    col_names = [
        "rid", "changed", "user", "enabled", "name", "type", "currency",
        "operatorid", "bik", "bank", "about", "filialid", "balance",
        "transactions", "bin"
    ]
    idx_changed = col_names.index("changed")

    total = 0
    t_start = time.time()

    try:
        with mysql_connection.cursor() as cur:
            print("Executing MySQL dict31 query...")
            t_exec = time.time()
            cur.execute(src_sql)
            print(f"MySQL dict31 execute OK in {time.time()-t_exec:.2f}s. Fetching...")

            while True:
                t_fetch = time.time()
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                print(f"fetchmany(dict31) got {len(rows)} rows in {time.time()-t_fetch:.2f}s")

                fixed = []
                for r in rows:
                    rr = list(r)
                    rr[idx_changed] = to_dt(rr[idx_changed], col="dict31.changed")
                    fixed.append(tuple(rr))

                ch.insert(f"{ch_db}.{target_table}", fixed, column_names=col_names)

                total += len(fixed)
                elapsed = time.time() - t_start
                print(f"Inserted dict31: {total} rows | {elapsed:.1f}s | ~{total/elapsed:.0f} r/s")

    finally:
        mysql_connection.close()

    print_stats(prefix="[dict31] ")
    print(f"=== DONE dict31 | rows={total} ===")


def load_dict32():
    to_dt, print_stats = _make_dt_converters()

    mysql_schema, mysql_connection = _mysql_conn()
    ch_db, ch = _ch_client()

    target_table = "dict32"
    ch_fq = f"`{ch_db}`.`{target_table}`"

    print(f"=== LOAD {mysql_schema}.dict32 -> {ch_fq} | batch={BATCH_SIZE} ===")

    ch.command(f"TRUNCATE TABLE {ch_fq}")

    src_sql = f"""
    select
        rid,
        changed,
        user,
        enabled,
        bindrid,
        money,
        mode,
        qid,
        docid,
        doctemplateid,
        userid,
        datetime,
        first_datetime,
        msg
    from `{mysql_schema}`.`dict32`
    """

    col_names = [
        "rid", "changed", "user", "enabled", "bindrid", "money", "mode",
        "qid", "docid", "doctemplateid", "userid", "datetime",
        "first_datetime", "msg"
    ]
    idx = {c: i for i, c in enumerate(col_names)}

    total = 0
    t_start = time.time()

    try:
        with mysql_connection.cursor() as cur:
            print("Executing MySQL dict32 query...")
            t_exec = time.time()
            cur.execute(src_sql)
            print(f"MySQL dict32 execute OK in {time.time()-t_exec:.2f}s. Fetching...")

            while True:
                t_fetch = time.time()
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                print(f"fetchmany(dict32) got {len(rows)} rows in {time.time()-t_fetch:.2f}s")

                fixed = []
                for r in rows:
                    rr = list(r)
                    rr[idx["changed"]] = to_dt(rr[idx["changed"]], col="dict32.changed")
                    rr[idx["datetime"]] = to_dt(rr[idx["datetime"]], col="dict32.datetime")
                    rr[idx["first_datetime"]] = to_dt(rr[idx["first_datetime"]], col="dict32.first_datetime")
                    fixed.append(tuple(rr))

                ch.insert(f"{ch_db}.{target_table}", fixed, column_names=col_names)

                total += len(fixed)
                elapsed = time.time() - t_start
                print(f"Inserted dict32: {total} rows | {elapsed:.1f}s | ~{total/elapsed:.0f} r/s")

    finally:
        mysql_connection.close()

    print_stats(prefix="[dict32] ")
    print(f"=== DONE dict32 | rows={total} ===")


def build_dict31_flat():
    ch_db, ch = _ch_client()
    target_flat = f"`{ch_db}`.`dict31_flat`"
    t31 = f"`{ch_db}`.`dict31`"
    t32 = f"`{ch_db}`.`dict32`"

    print(f"=== BUILD FLAT {t31} + {t32} -> {target_flat} ===")

    ch.command(f"TRUNCATE TABLE {target_flat}")

    sql = f"""
    INSERT INTO {target_flat}
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
    FROM {t31} t
    LEFT JOIN {t32} t2 ON t.rid = t2.bindrid
    """

    t0 = time.time()
    ch.command(sql)
    elapsed = time.time() - t0
    cnt = ch.query(f"SELECT count() FROM {target_flat}").result_rows[0][0]
    print(f"BUILD OK | rows={cnt} | {elapsed:.2f}s")


with DAG(
    dag_id="sync_mysql_to_clickhouse_dict31_dict32_then_flat_serzhan2",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sync", "mysql", "clickhouse", "dict31", "dict32", "flat"],
) as dag:

    t_load_dict31 = PythonOperator(
        task_id="load_dict31",
        python_callable=load_dict31,
    )

    t_load_dict32 = PythonOperator(
        task_id="load_dict32",
        python_callable=load_dict32,
    )

    t_build_flat = PythonOperator(
        task_id="build_dict31_flat",
        python_callable=build_dict31_flat,
    )

    t_load_dict31 >> t_load_dict32 >> t_build_flat
