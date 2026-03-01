from __future__ import annotations

import time
from datetime import datetime, date, time as dtime
from collections import defaultdict, deque
from typing import Optional, Dict

import pymysql
import clickhouse_connect

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


MYSQL_CONN_ID = "tourservice_mysql"
CH_CONN_ID = "clickhouse"

MYSQL_SCHEMA = None
CH_DB = None

BATCH_SIZE_DEFAULT = 200_000
BATCH_SIZE_SMALL = 10_000


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


ZERO_DATE_STRINGS = {
    "0000-00-00",
    "0000-00-00 00:00:00",
    "0000-00-00 00:00:00.000000",
}


def _to_dt_min(x) -> Optional[datetime]:
    """Minimal converter: datetime/str/bytes -> datetime or None (zero-date -> None)."""
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
        return None

    return None


def sync_table_full(table: str, batch_size: int = BATCH_SIZE_SMALL):
    mysql_schema, mysql_connection = _mysql_conn()
    ch_db, ch = _ch_client()

    mysql_fq = f"`{mysql_schema}`.`{table}`"
    ch_fq = f"`{ch_db}`.`{table}`"

    print(f"=== FULL RELOAD {mysql_fq} -> {ch_fq} | batch={batch_size} ===")

    t0 = time.time()
    ch.command(f"TRUNCATE TABLE {ch_fq}")
    print(f"TRUNCATE OK in {time.time()-t0:.2f}s")

    total = 0
    t_start = time.time()

    try:
        with mysql_connection.cursor() as cur:
            cur.execute(f"SELECT * FROM {mysql_fq}")
            col_names = [d[0] for d in cur.description]
            print(f"MySQL columns ({len(col_names)}): {col_names}")

            batch = []
            batch_rows = 0

            while True:
                row = cur.fetchone()
                if row is None:
                    break

                batch.append(row)
                batch_rows += 1

                if batch_rows >= batch_size:
                    ch.insert(
                        table=f"{ch_db}.{table}",
                        data=batch,
                        column_names=col_names,
                    )
                    total += batch_rows
                    elapsed = time.time() - t_start
                    rps = total / elapsed if elapsed > 0 else 0
                    print(f"Inserted {total} rows | elapsed={elapsed:.2f}s | ~{rps:.0f} rows/s")
                    batch = []
                    batch_rows = 0

            if batch_rows > 0:
                ch.insert(
                    table=f"{ch_db}.{table}",
                    data=batch,
                    column_names=col_names,
                )
                total += batch_rows

    finally:
        mysql_connection.close()

    elapsed = time.time() - t_start
    rps = total / elapsed if elapsed > 0 else 0
    ch_count = ch.query(f"SELECT count() FROM {ch_fq}").result_rows[0][0]

    print(
        f"=== DONE {table} ===\n"
        f"MySQL read rows: {total}\n"
        f"ClickHouse count: {ch_count}\n"
        f"Time: {elapsed:.2f}s\n"
        f"Speed: ~{rps:.0f} rows/s\n"
    )


def load_dict31(batch_size: int = BATCH_SIZE_DEFAULT):
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
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break

                fixed = []
                for r in rows:
                    rr = list(r)
                    rr[idx_changed] = _to_dt_min(rr[idx_changed])
                    fixed.append(tuple(rr))

                ch.insert(f"{ch_db}.dict31", fixed, column_names=col_names)
                total += len(fixed)

                elapsed = time.time() - t0
                print(f"dict31 inserted={total} | {elapsed:.1f}s | ~{total/elapsed:.0f} r/s")

    finally:
        mysql_connection.close()

    print(f"=== DONE dict31 rows={total} ===")


def load_dict32(batch_size: int = BATCH_SIZE_DEFAULT):
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
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break

                fixed = []
                for r in rows:
                    rr = list(r)
                    rr[idx_changed] = _to_dt_min(rr[idx_changed])
                    rr[idx_dt] = _to_dt_min(rr[idx_dt])
                    rr[idx_first] = _to_dt_min(rr[idx_first])
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
    print(f"=== DONE BUILD dict31_flat | rows={cnt} | {time.time()-t0:.2f}s ===")


def load_dict3_flat(batch_size: int = BATCH_SIZE_DEFAULT):
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

            s10 = s[:10]
            for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
                try:
                    return datetime.strptime(s10, fmt).date()
                except ValueError:
                    pass

            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
                try:
                    return datetime.strptime(s[:26], fmt).date()
                except ValueError:
                    pass

            parse_fail_by_col[col] += 1
            _mark_sample(col, s)
            return None

        type_fail_by_col[col] += 1
        _mark_sample(col, x)
        return None

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

    mysql_schema, mysql_connection = _mysql_conn()
    ch_db, ch = _ch_client()

    target_table = "dict3_flat"
    ch_fq = f"`{ch_db}`.`{target_table}`"

    print(f"=== LOAD FLAT {mysql_schema}.dict3 + {mysql_schema}.dict4 -> {ch_fq} | batch={batch_size} ===")

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
        "d3_rid","d3_changed","d3_user","d3_enabled","d3_orgname","d3_orgtype","d3_orgdate","d3_country",
        "d3_town","d3_address","d3_address2","d3_phone","d3_email","d3_site","d3_bankinfo","d3_member",
        "d3_iik","d3_bik","d3_bin","d3_kbe",
        "d4_rid","d4_changed","d4_user","d4_enabled","d4_bindrid","d4_commission","d4_guarantee",
        "d4_guarantee_num","d4_guarantee_date","d4_chieffname","d4_agreement","d4_created","d4_status",
        "d4_about","d4_tourfirmname","d4_filials","d4_licence","d4_founders","d4_insurance","d4_offices",
        "d4_cellphone","d4_bad_past_tour","d4_create_past_tour","d4_no_create_tour","d4_allow_auto_tour",
        "d4_list","d4_is_agent","d4_remarks","d4_auto_bad_tour","d4_hajj","d4_description",
    ]
    idx = {name: i for i, name in enumerate(col_names)}

    total = 0
    t_start = time.time()

    try:
        with mysql_connection.cursor() as cur:
            cur.execute(src_sql)
            while True:
                rows = cur.fetchmany(batch_size)
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
        f"=== DONE dict3_flat ===\n"
        f"MySQL read rows: {total}\n"
        f"ClickHouse count: {ch_count}\n"
        f"Time: {elapsed:.2f}s\n"
        f"Speed: ~{rps:.0f} rows/s\n"
        f"Zero-date by column: {_fmt_dict(zero_by_col)}\n"
        f"Parse-fail by column: {_fmt_dict(parse_fail_by_col)}\n"
        f"Type-fail by column: {_fmt_dict(type_fail_by_col)}\n"
    )

    if sum(parse_fail_by_col.values()) > 0 or sum(type_fail_by_col.values()) > 0 or sum(zero_by_col.values()) > 0:
        for col in sorted(samples_by_col.keys()):
            if samples_by_col[col]:
                print(f"Samples[{col}]: {list(samples_by_col[col])}")


def load_dict90(batch_size: int = BATCH_SIZE_DEFAULT):
    mysql_schema, mysql_connection = _mysql_conn()
    ch_db, ch = _ch_client()

    target_table = "dict90"
    ch_fq = f"`{ch_db}`.`{target_table}`"
    print(f"=== LOAD {mysql_schema}.dict90 -> {ch_fq} | batch={batch_size} ===")

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
        "rid","number","country1_id","country2_id","country3_id","country4_id","country5_id","country6_id",
        "currency","date_start","date_end","touragent_bin","airport_start","airport_end","flight_start","flight_end",
        "airlines","from_cabinet","passport","tid","qid","created","changed","user","enabled",
    ]

    total = 0
    t_start = time.time()

    try:
        with mysql_connection.cursor() as cur:
            cur.execute(src_sql)
            while True:
                rows = cur.fetchmany(batch_size)
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


def load_dict91(batch_size: int = BATCH_SIZE_DEFAULT):
    mysql_schema, mysql_connection = _mysql_conn()
    ch_db, ch = _ch_client()

    target_table = "dict91"
    ch_fq = f"`{ch_db}`.`{target_table}`"
    print(f"=== LOAD {mysql_schema}.dict91 -> {ch_fq} | batch={batch_size} ===")

    t0 = time.time()
    ch.command(f"TRUNCATE TABLE {ch_fq}")
    print(f"TRUNCATE OK in {time.time()-t0:.2f}s")

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
        "rid","bindrid","sub_date_start","sub_date_end","sub_airport","sub_airlines","sub_flight",
        "changed","user","enabled",
    ]

    total = 0
    t_start = time.time()

    try:
        with mysql_connection.cursor() as cur:
            cur.execute(src_sql)
            while True:
                rows = cur.fetchmany(batch_size)
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


def build_dict90_flat():
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


def _dashboard_inserts(ch_db: str) -> Dict[str, str]:
    """
    Map: dashboard_table_name -> INSERT SELECT sql
    IMPORTANT: SQL includes target table name.
    """
    d5 = f"`{ch_db}`.`dashboard_5`"
    d7 = f"`{ch_db}`.`dashboard_7`"
    d9 = f"`{ch_db}`.`dashboard_9`"
    d11 = f"`{ch_db}`.`dashboard_11`"
    d12 = f"`{ch_db}`.`dashboard_12`"
    d19 = f"`{ch_db}`.`dashboard_19`"

    dict3_flat = f"`{ch_db}`.`dict3_flat`"
    dict31_flat = f"`{ch_db}`.`dict31_flat`"
    dict90_flat = f"`{ch_db}`.`dict90_flat`"
    dict13 = f"`{ch_db}`.`dict13`"
    dict14 = f"`{ch_db}`.`dict14`"
    dict15 = f"`{ch_db}`.`dict15`"
    dict59 = f"`{ch_db}`.`dict59`"
    


    sql_5 = f"""
    insert into {d5}
    select
        t3.created                              as created,
        t3.number                               as tourcode,
        concat(
            'https://report.fondkamkor.kz/Voucher/queries/',
            toString(t3.number),
            '/view'
        )                                       as tourcode_url,
        t.d3_orgname                            as touragent,
        t3.qid                                  as qid,
        t3.date_start                           as date_start,
        t3.date_end                             as date_end,
        t3.airlines                             as airlines,
        t3.airport_start                        as airport_kz,
        t3.airport_end                          as airport_dest,
        t4.country                              as country,
        t3.passport                             as passport,
        t.d4_description                        as note,
        t3.sub_date_start                       as sub_date_start,
        t3.sub_date_end                         as sub_date_end,
        t3.sub_airlines                         as sub_airlines,
        t3.sub_airport                          as sub_airport
    from {dict3_flat} t join {dict31_flat} t2 on t.d4_rid = t2.d31_operatorid
    left join {dict90_flat} t3                on t3.tid = t.d4_rid AND t3.qid = t2.d32_qid
    left join {dict13} t4                     on t3.country1_id = t4.rid
    where 1=1
      and t2.d32_enabled = 1
      and t2.d32_mode = 0
      and t2.d32_qid > 0
      and toYear(t3.created) != 1970
    """

    sql_7 = f"""
    insert into {d7}
    select
	    t3.created    	    	    as created,
	    toYear(t3.created)			as year,
	    toMonth(t3.created)			as month,
	    t2.d32_qid 					as qid,
		t.d3_orgname				as orgname,
		t3.airport_start			as airport,
		t5.town						as city,
		t7.country					as country 
    from {dict3_flat} t 
    join {dict31_flat} t2                     on t.d4_rid=t2.d31_operatorid
    left join {dict90_flat} t3                on t3.tid=t.d4_rid and t3.qid=t2.d32_qid
    left join {dict59} t4  	                  on t4.iata=t3.airport_start
    left join {dict15} t5 		              on t5.rid=t4.bindrid 
    left join {dict14} t6 		              on t6.rid=t5.bindrid 
    left join {dict13} t4                     on t3.country1_id=t4.rid
    where 1=1
      and t2.d32_enabled = 1
      and t2.d32_mode = 0
      and t2.d32_qid > 0
      and toYear(t3.created) != 1970
    """  

    sql_9 = f"""
    insert into {d9}
    select
	    t3.created    	    	    as created,
	    toYear(t3.created)			as year,
	    toMonth(t3.created)			as month,
	    case when toMonth(t3.created)=1 then 'Январь'
	    	 when toMonth(t3.created)=2 then 'Февраль'
	    	 when toMonth(t3.created)=3 then 'Март'
	    	 when toMonth(t3.created)=4 then 'Апрель'
	    	 when toMonth(t3.created)=5 then 'Май'
	    	 when toMonth(t3.created)=6 then 'Июнь'
	    	 when toMonth(t3.created)=7 then 'Июль'
	    	 when toMonth(t3.created)=8 then 'Август'
	    	 when toMonth(t3.created)=9 then 'Сентябрь'
	    	 when toMonth(t3.created)=10 then 'Октябрь'
	    	 when toMonth(t3.created)=11 then 'Ноябрь'
	    	 when toMonth(t3.created)=12 then 'Декабрь'
	    	 else null
	    end 						as month_russian,
	    t2.d32_qid 					as qid,
		t3.airport_start			as airport,
		t7.country					as country,
		t5.town						as city
    from {dict3_flat} t 
    join {dict31_flat} t2 	 	on t.d4_rid=t2.d31_operatorid
    left join {dict90_flat} t3  on t3.tid=t.d4_rid and t3.qid=t2.d32_qid       
    left join {dict59} t4  	    on t4.iata=t3.airport_start
    left join {dict15} t5 		on t5.rid=t4.bindrid 
    left join {dict14} t6 		on t6.rid=t5.bindrid 
    left join {dict13} t7 		on t7.rid=t6.bindrid 
    where 1=1
      and t2.d32_enabled = 1
      and t2.d32_mode = 0
      and t2.d32_qid > 0
      and toYear(t3.created) != 1970
    """

    sql_11 = f"""
    insert into {d11}
    select
    	    t3.created    	    	    as created,
    	    toYear(t3.created)			as year,
    	    toMonth(t3.created)			as month,
    	    case when toMonth(t3.created)=1 then 'Январь'
    	    	 when toMonth(t3.created)=2 then 'Февраль'
    	    	 when toMonth(t3.created)=3 then 'Март'
    	    	 when toMonth(t3.created)=4 then 'Апрель'
    	    	 when toMonth(t3.created)=5 then 'Май'
    	    	 when toMonth(t3.created)=6 then 'Июнь'
    	    	 when toMonth(t3.created)=7 then 'Июль'
    	    	 when toMonth(t3.created)=8 then 'Август'
    	    	 when toMonth(t3.created)=9 then 'Сентябрь'
    	    	 when toMonth(t3.created)=10 then 'Октябрь'
    	    	 when toMonth(t3.created)=11 then 'Ноябрь'
    	    	 when toMonth(t3.created)=12 then 'Декабрь'
    	    	 else null
    	    end 						as month_russian,
    		t.d3_orgname				as orgname,
    		t2.d32_qid 					as qid
    from {dict3_flat} t 
    join {dict31_flat} t2                     on t.d4_rid=t2.d31_operatorid
    left join {dict90_flat} t3                on t3.tid=t.d4_rid and t3.qid=t2.d32_qid
    left join {dict13} t4                     on t3.country1_id=t4.rid
    where 1=1
      and t2.d32_enabled = 1
      and t2.d32_mode = 0
      and t2.d32_qid > 0
      and toYear(t3.created) != 1970
    """      
    
    sql_12 = f"""
    insert into {d12}
    select
        t3.created                           as created,
        toYear(created)                      as year,
        t.d3_orgname                         as orgname,
        t.d4_allow_auto_tour                 as allow_auto_tour,
        case when t.d4_allow_auto_tour=1 then 'Да' 
             else 'Нет' 
        end                                  as allow_auto_tour_russian,
        t3.from_cabinet                      as from_cabinet,
        t.d4_list                            as list,
        case when t.d4_list=0 then 'Туроператоры'
             when t.d4_list=1 then 'Фрахтователи'
             when t.d4_list=2 then 'Вышедшие туроператоры'
             when t.d4_list=3 then 'Приостановившиеся деятельность'
             when t.d4_list=4 then 'Скрытые'
             else null
        end                                  as list_russian,
        t3.passport                          as passport,
        t2.d32_qid                           as qid
    from {dict3_flat} t
    join {dict31_flat} t2      on t.d4_rid = t2.d31_operatorid
    left join {dict90_flat} t3 on t3.tid = t.d4_rid and t3.qid = t2.d32_qid
    left join {dict13} t4      on t3.country1_id = t4.rid
    where 1=1
      and t2.d32_enabled = 1
      and t2.d32_mode = 0
      and t2.d32_qid > 0
      and toYear(t3.created) != 1970
    """

    sql_19 = f"""
    insert into {d19}
    select
        t3.created                              as created,
        toYear(t3.created)                      as year,
        toMonth(t3.created)                     as month,
        case when toMonth(t3.created)=1  then 'Январь'
             when toMonth(t3.created)=2  then 'Февраль'
             when toMonth(t3.created)=3  then 'Март'
             when toMonth(t3.created)=4  then 'Апрель'
             when toMonth(t3.created)=5  then 'Май'
             when toMonth(t3.created)=6  then 'Июнь'
             when toMonth(t3.created)=7  then 'Июль'
             when toMonth(t3.created)=8  then 'Август'
             when toMonth(t3.created)=9  then 'Сентябрь'
             when toMonth(t3.created)=10 then 'Октябрь'
             when toMonth(t3.created)=11 then 'Ноябрь'
             when toMonth(t3.created)=12 then 'Декабрь'
             else null
        end                                     as month_russian,
        t.d3_orgname                            as orgname,
        t2.d32_qid                              as qid,
        t4.country                              as country
    from {dict3_flat} t
    join {dict31_flat} t2      on t.d4_rid = t2.d31_operatorid
    left join {dict90_flat} t3 on t3.tid = t.d4_rid AND t3.qid = t2.d32_qid
    left join {dict13} t4      on t3.country1_id = t4.rid
    where 1=1
      and t2.d32_enabled = 1
      and t2.d32_mode = 0
      and t2.d32_qid > 0
      and toYear(t3.created) != 1970
    """

    return {
        "dashboard_5": sql_5,
        "dashboard_7": sql_7,
        "dashboard_9": sql_9,
        "dashboard_11": sql_11,
        "dashboard_12": sql_12,
        "dashboard_19": sql_19,
    }


def refresh_one_dashboard(table: str):
    """
    TRUNCATE + INSERT for a single dashboard table.
    """
    ch_db, ch = _ch_client()

    inserts = _dashboard_inserts(ch_db)
    if table not in inserts:
        raise ValueError(f"Unknown dashboard table: {table}. Known: {list(inserts.keys())}")

    fq = f"`{ch_db}`.`{table}`"
    sql_insert = inserts[table]

    print(f"=== REFRESH {fq} (TRUNCATE + INSERT) ===")

    t0 = time.time()
    ch.command(f"TRUNCATE TABLE {fq}")
    print(f"TRUNCATE OK in {time.time() - t0:.2f}s")

    t1 = time.time()
    ch.command(sql_insert)
    print(f"INSERT OK in {time.time() - t1:.2f}s")

    cnt = ch.query(f"SELECT count() FROM {fq}").result_rows[0][0]
    print(f"=== DONE {fq} | rows={cnt} | total={time.time() - t0:.2f}s ===")


default_args = {
    "owner": "serzhan",
    "retries": 2,
    "retry_delay": 60,
}

with DAG(
    dag_id="mysql_to_clickhouse_superset_serzhan",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["sync", "mysql", "clickhouse", "tourservice", "full_reload", "dashboards"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup(group_id="dicts_basic_fullreload") as g_basic:
        t13 = PythonOperator(task_id="sync_dict13", python_callable=sync_table_full, op_kwargs={"table": "dict13"})
        t14 = PythonOperator(task_id="sync_dict14", python_callable=sync_table_full, op_kwargs={"table": "dict14"})
        t15 = PythonOperator(task_id="sync_dict15", python_callable=sync_table_full, op_kwargs={"table": "dict15"})
        t59 = PythonOperator(task_id="sync_dict59", python_callable=sync_table_full, op_kwargs={"table": "dict59"})
        t13 >> t14 >> t15 >> t59

    with TaskGroup(group_id="dict31_32_and_flat") as g_31_32:
        t31 = PythonOperator(task_id="load_dict31", python_callable=load_dict31)
        t32 = PythonOperator(task_id="load_dict32", python_callable=load_dict32)
        t31flat = PythonOperator(task_id="build_dict31_flat", python_callable=build_dict31_flat)
        t31 >> t32 >> t31flat

    with TaskGroup(group_id="dict90_91_and_flat") as g_90_91:
        t90 = PythonOperator(task_id="load_dict90", python_callable=load_dict90)
        t91 = PythonOperator(task_id="load_dict91", python_callable=load_dict91)
        t90flat = PythonOperator(task_id="build_dict90_flat", python_callable=build_dict90_flat)
        t90 >> t91 >> t90flat

    with TaskGroup(group_id="dict3_flat_group") as g_3flat:
        t3flat = PythonOperator(task_id="load_dict3_flat", python_callable=load_dict3_flat)

    with TaskGroup(group_id="refresh_dashboards") as g_dash:
        d5 = PythonOperator(
            task_id="refresh_dashboard_5",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "dashboard_5"},
        )
        d7 = PythonOperator(
            task_id="refresh_dashboard_7",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "dashboard_7"},
        )
        d9 = PythonOperator(
            task_id="refresh_dashboard_9",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "dashboard_9"},
        )
        d7 = PythonOperator(
            task_id="refresh_dashboard_11",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "dashboard_11"},
        )
        d12 = PythonOperator(
            task_id="refresh_dashboard_12",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "dashboard_12"},
        )
        d19 = PythonOperator(
            task_id="refresh_dashboard_19",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "dashboard_19"},
        )

        d5 >> d7 >> d9 >> d11 >> d12 >> d19

    start >> [g_basic, g_31_32, g_90_91, g_3flat] >> g_dash >> end
