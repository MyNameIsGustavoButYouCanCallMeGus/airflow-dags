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


# -------------------------
# Type normalization + stats
# -------------------------

ZERO_DATE_STRINGS = {"0000-00-00", "0000-00-00 00:00:00", "0000-00-00 00:00:00.000000"}


def _decode_if_bytes(x):
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", errors="ignore")
    return x


def load_dict3_flat():
    # stats
    zero_by_col = defaultdict(int)
    parse_fail_by_col = defaultdict(int)
    type_fail_by_col = defaultdict(int)
    samples_by_col = defaultdict(lambda: deque(maxlen=5))

    def _mark_sample(col, value):
        try:
            samples_by_col[col].append(value if isinstance(value, str) else repr(value))
        except Exception:
            samples_by_col[col].append("<sample_error>")

    def _to_date(x, col):
        """
        Returns datetime.date or None.
        Never raises (so ETL doesn't die on one bad value).
        Updates counters for diagnostics.
        """
        if x is None:
            return None

        x = _decode_if_bytes(x)

        if isinstance(x, date) and not isinstance(x, datetime):
            return x

        if isinstance(x, datetime):
            return x.date()

        if isinstance(x, str):
            s = x.strip()
            if not s:
                return None

            if s in ZERO_DATE_STRINGS:
                zero_by_col[col] += 1
                _mark_sample(col, s)
                return None

            # try common formats
            # if datetime-like, take first 10 chars
            s10 = s[:10]
            for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
                try:
                    return datetime.strptime(s10, fmt).date()
                except ValueError:
                    pass

            # maybe it's full datetime string but we only need date
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
                try:
                    return datetime.strptime(s[:26], fmt).date()
                except ValueError:
                    pass

            # parse fail
            parse_fail_by_col[col] += 1
            _mark_sample(col, s)
            return None

        # unexpected type
        type_fail_by_col[col] += 1
        _mark_sample(col, x)
        return None

    def _to_dt(x, col):
        """
        Returns datetime.datetime or None.
        Never raises.
        Updates counters for diagnostics.
        """
        if x is None:
            return None

        x = _decode_if_bytes(x)

        if isinstance(x, datetime):
            return x

        if isinstance(x, date):
            # convert date -> datetime at 00:00:00
            return datetime.combine(x, dtime.min)

        if isinstance(x, str):
            s = x.strip()
            if not s:
                return None

            # zero-date-like
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

    mysql_schema, mysql_connection = _mysql_conn()
    ch_db, ch = _ch_client()

    target_table = "dict3_flat"
    ch_fq = f"`{ch_db}`.`{target_table}`"

    print(f"=== LOAD FLAT {mysql_schema}.dict3 + {mysql_schema}.dict4 -> {ch_fq} | batch={BATCH_SIZE} ===")

    t0 = time.time()
    ch.command(f"TRUNCATE TABLE {ch_fq}")
    print(f"TRUNCATE OK in {time.time() - t0:.2f}s")

    src_sql = f"""
    select
            t.rid               as d3_rid,
            t.changed           as d3_changed,
            t.user              as d3_user,
            t.enabled           as d3_enabled,
            t.orgname           as d3_orgname,
            t.orgtype           as d3_orgtype,
            t.orgdate           as d3_orgdate,
            t.country           as d3_country,
            t.town              as d3_town,
            t.address           as d3_address,
            t.address2          as d3_address2,
            t.phone             as d3_phone,
            t.email             as d3_email,
            t.site              as d3_site,
            t.bankinfo          as d3_bankinfo,
            t.member            as d3_member,
            t.iik               as d3_iik,
            t.bik               as d3_bik,
            t.bin               as d3_bin,
            t.kbe               as d3_kbe,
            t2.rid              as d4_rid,
            t2.changed          as d4_changed,
            t2.user             as d4_user,
            t2.enabled          as d4_enabled,
            t2.bindrid          as d4_bindrid,
            t2.commission       as d4_commission,
            t2.guarantee        as d4_guarantee,
            t2.guarantee_num    as d4_guarantee_num,
            t2.guarantee_date   as d4_guarantee_date,
            t2.chieffname       as d4_chieffname,
            t2.agreement        as d4_agreement,
            t2.created          as d4_created,
            t2.status           as d4_status,
            t2.about            as d4_about,
            t2.tourfirmname     as d4_tourfirmname,
            t2.filials          as d4_filials,
            t2.licence          as d4_licence,
            t2.founders         as d4_founders,
            t2.insurance        as d4_insurance,
            t2.offices          as d4_offices,
            t2.cellphone        as d4_cellphone,
            t2.bad_past_tour    as d4_bad_past_tour,
            t2.create_past_tour as d4_create_past_tour,
            t2.no_create_tour   as d4_no_create_tour,
            t2.allow_auto_tour  as d4_allow_auto_tour,
            t2.list             as d4_list,
            t2.is_agent         as d4_is_agent,
            t2.remarks          as d4_remarks,
            t2.auto_bad_tour    as d4_auto_bad_tour,
            t2.hajj             as d4_hajj,
            t2.description      as d4_description
    from `{mysql_schema}`.`dict3` t
    left join `{mysql_schema}`.`dict4` t2
        on t.rid = t2.bindrid
    """

    col_names = [
        "d3_rid",
        "d3_changed",
        "d3_user",
        "d3_enabled",
        "d3_orgname",
        "d3_orgtype",
        "d3_orgdate",
        "d3_country",
        "d3_town",
        "d3_address",
        "d3_address2",
        "d3_phone",
        "d3_email",
        "d3_site",
        "d3_bankinfo",
        "d3_member",
        "d3_iik",
        "d3_bik",
        "d3_bin",
        "d3_kbe",
        "d4_rid",
        "d4_changed",
        "d4_user",
        "d4_enabled",
        "d4_bindrid",
        "d4_commission",
        "d4_guarantee",
        "d4_guarantee_num",
        "d4_guarantee_date",
        "d4_chieffname",
        "d4_agreement",
        "d4_created",
        "d4_status",
        "d4_about",
        "d4_tourfirmname",
        "d4_filials",
        "d4_licence",
        "d4_founders",
        "d4_insurance",
        "d4_offices",
        "d4_cellphone",
        "d4_bad_past_tour",
        "d4_create_past_tour",
        "d4_no_create_tour",
        "d4_allow_auto_tour",
        "d4_list",
        "d4_is_agent",
        "d4_remarks",
        "d4_auto_bad_tour",
        "d4_hajj",
        "d4_description",
    ]

    # indexes
    idx = {name: i for i, name in enumerate(col_names)}

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

                    rr[idx["d3_changed"]] = _to_dt(rr[idx["d3_changed"]], col="d3_changed")
                    rr[idx["d3_orgdate"]] = _to_date(rr[idx["d3_orgdate"]], col="d3_orgdate")

                    rr[idx["d4_changed"]] = _to_dt(rr[idx["d4_changed"]], col="d4_changed")
                    rr[idx["d4_guarantee_date"]] = _to_date(rr[idx["d4_guarantee_date"]], col="d4_guarantee_date")
                    rr[idx["d4_created"]] = _to_date(rr[idx["d4_created"]], col="d4_created")
                    rr[idx["d4_hajj"]] = _to_date(rr[idx["d4_hajj"]], col="d4_hajj")

                    fixed_rows.append(tuple(rr))

                ch.insert(
                    table=f"{ch_db}.{target_table}",
                    data=fixed_rows,
                    column_names=col_names,
                )

                total += len(fixed_rows)
                elapsed = time.time() - t_start
                rps = total / elapsed if elapsed > 0 else 0

                # summary counters
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

    # final diagnostics
    def _fmt_dict(d):
        return "{ " + ", ".join(f"{k}: {v}" for k, v in sorted(d.items())) + " }"

    print(
        f"=== DONE dict3_flat ===\n"
        f"MySQL read rows: {total}\n"
        f"ClickHouse count: {ch_count}\n"
        f"Time: {elapsed:.2f}s\n"
        f"Speed: ~{rps:.0f} rows/s\n"
        f"Zero-date by column: {_fmt_dict(zero_by_col)}\n"
        f"Parse-fail by column: {_fmt_dict(parse_fail_by_col)}\n"
        f"Type-fail by column: {_fmt_dict(type_fail_by_col)}\n"
    )

    # print samples only if something suspicious happened
    if sum(parse_fail_by_col.values()) > 0 or sum(type_fail_by_col.values()) > 0 or sum(zero_by_col.values()) > 0:
        for col in sorted(samples_by_col.keys()):
            if samples_by_col[col]:
                print(f"Samples[{col}]: {list(samples_by_col[col])}")


with DAG(
    dag_id="sync_mysql_to_clickhouse_dict3_flat_serzhan2",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sync", "mysql", "clickhouse", "flat", "dict3"],
) as dag:

    load_flat = PythonOperator(
        task_id="load_dict3_flat",
        python_callable=load_dict3_flat,
    )
