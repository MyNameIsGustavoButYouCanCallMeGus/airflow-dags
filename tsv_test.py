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

MYSQL_SCHEMA = None   # если None, возьмётся conn.schema (у тебя www)
CH_DB = None          # если None, возьмётся conn.schema (у тебя fondkamkor)

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


def load_dict31_flat():
    # ---------------- stats ----------------
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
        """
        Returns datetime.datetime or None.
        Never raises: bad values -> None + counters.
        """
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

            # common mysql datetime formats
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
                try:
                    return datetime.strptime(s[:26], fmt)
                except ValueError:
                    pass

            # ISO-like
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

    # ---------------- connections ----------------
    mysql_schema, mysql_connection = _mysql_conn()
    ch_db, ch = _ch_client()

    target_table = "dict31_flat"
    ch_fq = f"`{ch_db}`.`{target_table}`"

    print(f"=== LOAD FLAT {mysql_schema}.dict31 + {mysql_schema}.dict32 -> {ch_fq} | batch={BATCH_SIZE} ===")

    t0 = time.time()
    ch.command(f"TRUNCATE TABLE {ch_fq}")
    print(f"TRUNCATE OK in {time.time() - t0:.2f}s")

    # ---------------- source SQL (MySQL) ----------------
    src_sql = f"""
    select
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
    from `{mysql_schema}`.`dict31` t
    left join `{mysql_schema}`.`dict32` t2
        on t.rid = t2.bindrid
    """

    # ---------------- target columns ----------------
    col_names = [
        "d31_rid",
        "d31_changed",
        "d31_user",
        "d31_enabled",
        "d31_name",
        "d31_type",
        "d31_currency",
        "d31_operatorid",
        "d31_bik",
        "d31_bank",
        "d31_about",
        "d31_filialid",
        "d31_balance",
        "d31_transactions",
        "d31_bin",
        "d32_rid",
        "d32_changed",
        "d32_user",
        "d32_enabled",
        "d32_bindrid",
        "d32_money",
        "d32_mode",
        "d32_qid",
        "d32_docid",
        "d32_doctemplateid",
        "d32_userid",
        "d32_datetime",
        "d32_first_datetime",
        "d32_msg",
    ]
    idx = {name: i for i, name in enumerate(col_names)}

    # which columns are datetime
    dt_cols = ["d31_changed", "d32_changed", "d32_datetime", "d32_first_datetime"]

    total = 0
    t_start = time.time()

    try:
        with mysql_connection.cursor() as cur:
            cur.execute(src_sql)

            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break

                fixed_rows = []
                for r in rows:
                    rr = list(r)

                    for c in dt_cols:
                        rr[idx[c]] = _to_dt(rr[idx[c]], col=c)

                    fixed_rows.append(tuple(rr))

                ch.insert(
                    table=f"{ch_db}.{target_table}",
                    data=fixed_rows,
                    column_names=col_names,
                )

                total += len(fixed_rows)
                elapsed = time.time() - t_start
                rps = total / elapsed if elapsed > 0 else 0

                z_total = sum(zero_by_col.values())
                pf_total = sum(parse_fail_by_col.values())
                tf_total = sum(type_fail_by_col.values())

                print(
                    f"Inserted {total} rows | elapsed={elapsed:.1f}s | ~{rps:.0f} rows/s | "
                    f"zero={z_total} | parse_fail={pf_total} | type_fail={tf_total}"
                )

    finally:
        mysql_connection.close()

    elapsed = time.time() - t_start
    rps = total / elapsed if elapsed > 0 else 0
    ch_count = ch.query(f"select count() from {ch_fq}").result_rows[0][0]

    def _fmt_dict(d):
        return "{ " + ", ".join(f"{k}: {v}" for k, v in sorted(d.items())) + " }"

    print(
        f"=== DONE dict31_flat ===\n"
        f"MySQL read rows: {total}\n"
        f"ClickHouse count: {ch_count}\n"
        f"Time: {elapsed:.2f}s\n"
        f"Speed: ~{rps:.0f} rows/s\n"
        f"Zero-date by column: {_fmt_dict(zero_by_col)}\n"
        f"Parse-fail by column: {_fmt_dict(parse_fail_by_col)}\n"
        f"Type-fail by column: {_fmt_dict(type_fail_by_col)}\n"
    )

    if sum(zero_by_col.values()) or sum(parse_fail_by_col.values()) or sum(type_fail_by_col.values()):
        for col in sorted(samples_by_col.keys()):
            if samples_by_col[col]:
                print(f"Samples[{col}]: {list(samples_by_col[col])}")


with DAG(
    dag_id="sync_mysql_to_clickhouse_dict31_flat_serzhan",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sync", "mysql", "clickhouse", "flat", "dict31", "dict32"],
) as dag:

    load_flat = PythonOperator(
        task_id="load_dict31_flat",
        python_callable=load_dict31_flat,
    )
