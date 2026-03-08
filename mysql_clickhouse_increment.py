import os
###final update5
import time
import re
import calendar
from datetime import datetime, date, time as dtime, timedelta

import pymysql
import clickhouse_connect

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

os.environ["TZ"] = "UTC"
time.tzset()

MYSQL_CONN_ID = "tourservice_mysql"
CH_CONN_ID = "clickhouse"

MYSQL_SCHEMA = None
CH_DB = None

OVERLAP_MINUTES = 5
DEBUG_SAMPLE_ROWS = 20
MYSQL_BATCH_SIZE = 10_000

ZERO_DATE_STRINGS = {
    "0000-00-00",
    "0000-00-00 00:00:00",
    "0000-00-00 00:00:00.000000",
}


# =========================
# CONNECTIONS
# =========================
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


# =========================
# TYPE HELPERS
# =========================
def _decode_if_bytes(x):
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", errors="ignore")
    return x


def _unwrap_ch_type(ch_type: str) -> str:
    t = ch_type.strip()
    while True:
        m = re.match(r"^(Nullable|LowCardinality)\((.*)\)$", t)
        if not m:
            break
        t = m.group(2).strip()
    return t


def _to_date(x):
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
        if s in ZERO_DATE_STRINGS or s.startswith("0000-00-00"):
            return None

        for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
            try:
                return datetime.strptime(s[:10], fmt).date()
            except ValueError:
                pass

        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(s[:26], fmt).date()
            except ValueError:
                pass

        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                return datetime.strptime(s[:26], fmt).date()
            except ValueError:
                pass

        return None

    return None


def _to_dt(x):
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

        for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
            try:
                return datetime.combine(datetime.strptime(s[:10], fmt).date(), dtime.min)
            except ValueError:
                pass

        return None

    return None


def _to_ch_datetime_int(x):
    dt = _to_dt(x)
    if dt is None:
        return None
    return calendar.timegm(dt.timetuple())


# =========================
# CLICKHOUSE SCHEMA HELPERS
# =========================
def _get_ch_column_types(ch_client, raw_table: str) -> dict[str, str]:
    res = ch_client.query(f"DESCRIBE TABLE {raw_table}")
    out = {}
    for row in res.result_rows:
        col_name = row[0]
        col_type = row[1]
        out[col_name] = _unwrap_ch_type(col_type)
    return out


def _normalize_rows_by_ch_schema(rows, ch_col_types: dict[str, str]):
    fixed_rows = []
    for row in rows:
        fixed = dict(row)
        for col, ch_type in ch_col_types.items():
            if col not in fixed:
                continue

            if ch_type == "Date":
                fixed[col] = _to_date(fixed[col])
            elif ch_type == "DateTime":
                fixed[col] = _to_ch_datetime_int(fixed[col])
            elif ch_type.startswith("DateTime64"):
                fixed[col] = _to_dt(fixed[col])
            else:
                fixed[col] = _decode_if_bytes(fixed[col])

        fixed_rows.append(fixed)
    return fixed_rows


def _debug_temporal_columns(rows, ch_col_types: dict[str, str], label: str = ""):
    temporal_cols = [
        col for col, t in ch_col_types.items()
        if t == "Date" or t.startswith("DateTime")
    ]
    if not rows or not temporal_cols:
        return

    print(f"DEBUG temporal column sample types {label}:")
    for col in temporal_cols:
        vals = [r.get(col) for r in rows[:5] if col in r]
        types_ = [type(v).__name__ for v in vals]
        print(f"{col}: values={vals} | types={types_}")


# =========================
# WATERMARK HELPERS
# =========================
def _get_watermark(ch_client, table_name: str):
    wm_sql = f"""
        SELECT last_changed
        FROM etl_watermarks
        WHERE table_name = '{table_name}'
        ORDER BY updated_at DESC
        LIMIT 1
    """
    wm_res = ch_client.query(wm_sql)
    if not wm_res.result_rows:
        raise ValueError(f"Watermark for table {table_name} not found in etl_watermarks")

    wm = wm_res.result_rows[0][0]
    wm = _to_dt(wm)
    if wm is None:
        raise ValueError(f"Watermark for table {table_name} is invalid: {wm_res.result_rows[0][0]}")
    return wm


def _insert_watermark(ch_client, table_name: str, max_changed: datetime):
    ch_client.command(f"""
        INSERT INTO etl_watermarks (table_name, last_changed)
        VALUES ('{table_name}', toDateTime('{max_changed:%Y-%m-%d %H:%M:%S}'))
    """)


# =========================
# MAIN RAW LOAD FUNCTION
# =========================
def incremental_load_table(table_name: str, raw_table: str):
    print(f"=== START incremental load: {table_name} -> {raw_table} ===")

    _, ch_client = _ch_client()
    last_changed = _get_watermark(ch_client, table_name)
    last_changed_with_overlap = last_changed - timedelta(minutes=OVERLAP_MINUTES)

    print(f"Watermark: {last_changed}")
    print(f"Overlap watermark: {last_changed_with_overlap}")

    ch_col_types = _get_ch_column_types(ch_client, raw_table)
    print(f"ClickHouse schema for {raw_table}: {ch_col_types}")

    _, mysql = _mysql_conn()

    total_rows = 0
    global_max_changed = None
    debug_printed = False

    try:
        with mysql.cursor() as cursor:
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE changed >= %s
                ORDER BY changed, rid
            """
            cursor.execute(query, (last_changed_with_overlap,))

            while True:
                rows = cursor.fetchmany(MYSQL_BATCH_SIZE)

                if not rows:
                    break

                batch_size = len(rows)
                total_rows += batch_size
                print(f"Fetched batch: {batch_size} rows from MySQL for {table_name} (total={total_rows})")

                if not debug_printed:
                    for r in rows[:DEBUG_SAMPLE_ROWS]:
                        print(r)

                changed_values = [_to_dt(row.get("changed")) for row in rows if row.get("changed") is not None]
                changed_values = [x for x in changed_values if x is not None]
                if not changed_values:
                    raise ValueError(f"No valid changed values in current batch for table {table_name}")

                batch_max_changed = max(changed_values)
                if global_max_changed is None or batch_max_changed > global_max_changed:
                    global_max_changed = batch_max_changed

                rows = _normalize_rows_by_ch_schema(rows, ch_col_types)

                if not debug_printed:
                    _debug_temporal_columns(rows, ch_col_types, label=f"for {table_name} first batch")
                    debug_printed = True

                columns = [col for col in ch_col_types.keys() if col in rows[0]]
                data = [[row.get(col) for col in columns] for row in rows]

                ch_client.insert(
                    table=raw_table,
                    data=data,
                    column_names=columns,
                )

                print(
                    f"Inserted batch: {batch_size} rows into {raw_table} "
                    f"(total_inserted={total_rows}, batch_max_changed={batch_max_changed:%Y-%m-%d %H:%M:%S})"
                )

                del rows
                del data

        if total_rows == 0:
            print(f"No new rows for {table_name}")
            return

        if global_max_changed is None:
            raise ValueError(f"global_max_changed is None for table {table_name}")

        if global_max_changed > last_changed:
            _insert_watermark(ch_client, table_name, global_max_changed)
            print(
                f"Loaded {total_rows} rows into {raw_table}. "
                f"Watermark updated to {global_max_changed:%Y-%m-%d %H:%M:%S}"
            )
        else:
            print(
                f"Loaded {total_rows} overlap rows into {raw_table}. "
                f"Watermark not changed: {last_changed:%Y-%m-%d %H:%M:%S}"
            )

    finally:
        mysql.close()

    print(f"=== END incremental load: {table_name} ===")


# =========================
# FLAT BUILDERS FROM STAGE VIEWS
# =========================
def build_dict31_flat_from_stage():
    ch_db, ch = _ch_client()

    print(f"=== BUILD `{ch_db}`.`dict31_flat` FROM STAGE VIEWS ===")
    ch.command(f"TRUNCATE TABLE `{ch_db}`.`dict31_flat`")

    sql = f"""
    INSERT INTO `{ch_db}`.`dict31_flat`
    SELECT
        t.rid               AS d31_rid,
        t.changed           AS d31_changed,
        t.user              AS d31_user,
        t.enabled           AS d31_enabled,
        t.name              AS d31_name,
        t.type              AS d31_type,
        t.currency          AS d31_currency,
        t.operatorid        AS d31_operatorid,
        t.bik               AS d31_bik,
        t.bank              AS d31_bank,
        t.about             AS d31_about,
        t.filialid          AS d31_filialid,
        t.balance           AS d31_balance,
        t.transactions      AS d31_transactions,
        t.bin               AS d31_bin,
        t2.rid              AS d32_rid,
        t2.changed          AS d32_changed,
        t2.user             AS d32_user,
        t2.enabled          AS d32_enabled,
        t2.bindrid          AS d32_bindrid,
        t2.money            AS d32_money,
        t2.mode             AS d32_mode,
        t2.qid              AS d32_qid,
        t2.docid            AS d32_docid,
        t2.doctemplateid    AS d32_doctemplateid,
        t2.userid           AS d32_userid,
        t2.datetime         AS d32_datetime,
        t2.first_datetime   AS d32_first_datetime,
        t2.msg              AS d32_msg
    FROM `{ch_db}`.`dict31_stage` t
    LEFT JOIN `{ch_db}`.`dict32_stage` t2
        ON t.rid = t2.bindrid
    """

    t0 = time.time()
    ch.command(sql)
    cnt = ch.query(f"SELECT count() FROM `{ch_db}`.`dict31_flat`").result_rows[0][0]
    print(f"=== DONE BUILD dict31_flat | rows={cnt} | {time.time()-t0:.2f}s ===")


def build_dict3_flat_from_stage():
    ch_db, ch = _ch_client()

    print(f"=== BUILD `{ch_db}`.`dict3_flat` FROM STAGE VIEWS ===")
    ch.command(f"TRUNCATE TABLE `{ch_db}`.`dict3_flat`")

    debug_sql = f"""
    SELECT
        t.rid AS d3_rid,
        t2.rid AS d4_rid,
        t2.bindrid AS d4_bindrid,
        t2.guarantee_date AS raw_guarantee_date,
        toTypeName(t2.guarantee_date) AS raw_guarantee_date_type,
        toString(t2.guarantee_date) AS raw_guarantee_date_str,
        t2.created AS raw_created,
        toTypeName(t2.created) AS raw_created_type,
        toString(t2.created) AS raw_created_str,
        t2.hajj AS raw_hajj,
        toTypeName(t2.hajj) AS raw_hajj_type,
        toString(t2.hajj) AS raw_hajj_str
    FROM `{ch_db}`.`dict3_stage` t
    LEFT JOIN `{ch_db}`.`dict4_stage` t2
        ON t.rid = t2.bindrid
    WHERE
        t2.guarantee_date IS NULL
        OR toString(t2.guarantee_date) = ''
        OR toString(t2.guarantee_date) = '0000-00-00'
        OR toString(t2.guarantee_date) = '0000-00-00 00:00:00'
    LIMIT 100
    """

    print("===== DEBUG guarantee_date SQL START =====")
    print(debug_sql)
    print("===== DEBUG guarantee_date SQL END =====")

    debug_res = ch.query(debug_sql)

    print("===== DEBUG guarantee_date ROWS START =====")
    print(f"rows_found = {len(debug_res.result_rows)}")
    for i, row in enumerate(debug_res.result_rows, 1):
        print(f"row_{i} = {row}")
    print("===== DEBUG guarantee_date ROWS END =====")

    debug_cnt_sql = f"""
    SELECT count()
    FROM `{ch_db}`.`dict3_stage` t
    LEFT JOIN `{ch_db}`.`dict4_stage` t2
        ON t.rid = t2.bindrid
    WHERE
        t2.guarantee_date IS NULL
        OR toString(t2.guarantee_date) = ''
        OR toString(t2.guarantee_date) = '0000-00-00'
        OR toString(t2.guarantee_date) = '0000-00-00 00:00:00'
    """
    debug_cnt = ch.query(debug_cnt_sql).result_rows[0][0]
    print(f"problematic guarantee_date rows count = {debug_cnt}")

    raise ValueError("DEBUG STOP: printed problematic rows for guarantee_date")


def build_dict90_flat_from_stage():
    ch_db, ch = _ch_client()

    print(f"=== BUILD `{ch_db}`.`dict90_flat` FROM STAGE VIEWS ===")
    ch.command(f"TRUNCATE TABLE `{ch_db}`.`dict90_flat`")

    sql = f"""
    INSERT INTO `{ch_db}`.`dict90_flat`
    SELECT
        d.rid             AS rid,
        d.number          AS number,
        d.country1_id     AS country1_id,
        d.country2_id     AS country2_id,
        d.country3_id     AS country3_id,
        d.country4_id     AS country4_id,
        d.country5_id     AS country5_id,
        d.country6_id     AS country6_id,
        d.currency        AS currency,
        d.date_start      AS date_start,
        d.date_end        AS date_end,
        d.touragent_bin   AS touragent_bin,
        d.airport_start   AS airport_start,
        d.airport_end     AS airport_end,
        d.flight_start    AS flight_start,
        d.flight_end      AS flight_end,
        d.airlines        AS airlines,
        d.from_cabinet    AS from_cabinet,
        d.passport        AS passport,
        d.tid             AS tid,
        d.qid             AS qid,
        d.created         AS created,
        d.changed         AS changed,
        d.user            AS user,
        d.enabled         AS enabled,
        ifNull(d2.rid, 0) AS sub_rid,
        d2.bindrid        AS sub_bindrid,
        d2.sub_date_start AS sub_date_start,
        d2.sub_date_end   AS sub_date_end,
        d2.sub_airport    AS sub_airport,
        d2.sub_airlines   AS sub_airlines,
        d2.sub_flight     AS sub_flight,
        d2.changed        AS sub_changed,
        d2.user           AS sub_user,
        d2.enabled        AS sub_enabled
    FROM `{ch_db}`.`dict90_stage` d
    LEFT JOIN `{ch_db}`.`dict91_stage` d2
        ON d.rid = d2.bindrid
    """

    t0 = time.time()
    ch.command(sql)
    cnt = ch.query(f"SELECT count() FROM `{ch_db}`.`dict90_flat`").result_rows[0][0]
    print(f"=== DONE BUILD dict90_flat | rows={cnt} | {time.time()-t0:.2f}s ===")


# =========================
# DASHBOARD REFRESH
# =========================
def _dashboard_inserts(ch_db: str):
    d5 = f"`{ch_db}`.`t_so_dashboard_5`"
    d7 = f"`{ch_db}`.`t_so_dashboard_7`"
    d9 = f"`{ch_db}`.`t_so_dashboard_9`"
    d11 = f"`{ch_db}`.`t_so_dashboard_11`"
    d12 = f"`{ch_db}`.`t_so_dashboard_12`"
    d19 = f"`{ch_db}`.`t_so_dashboard_19`"

    dict3_flat = f"`{ch_db}`.`dict3_flat`"
    dict31_flat = f"`{ch_db}`.`dict31_flat`"
    dict90_flat = f"`{ch_db}`.`dict90_flat`"

    dict13_stage = f"`{ch_db}`.`dict13_stage`"
    dict14_stage = f"`{ch_db}`.`dict14_stage`"
    dict15_stage = f"`{ch_db}`.`dict15_stage`"
    dict59_stage = f"`{ch_db}`.`dict59_stage`"

    sql_5 = f"""
    INSERT INTO {d5}
    SELECT
        t3.created                              AS created,
        t3.number                               AS tourcode,
        concat(
            'https://report.fondkamkor.kz/Voucher/queries/',
            toString(t3.number),
            '/view'
        )                                       AS tourcode_url,
        t.d3_orgname                            AS orgname,
        t3.qid                                  AS qid,
        t3.date_start                           AS date_start,
        t3.date_end                             AS date_end,
        t3.airlines                             AS airlines,
        t3.airport_start                        AS airport_kz,
        t3.airport_end                          AS airport_dest,
        t4.country                              AS country,
        t3.passport                             AS passport,
        t.d4_description                        AS note,
        t3.sub_date_start                       AS sub_date_start,
        t3.sub_date_end                         AS sub_date_end,
        t3.sub_airlines                         AS sub_airlines,
        t3.sub_airport                          AS sub_airport
    FROM {dict3_flat} t
    JOIN {dict31_flat} t2
        ON t.d4_rid = t2.d31_operatorid
    LEFT JOIN {dict90_flat} t3
        ON t3.tid = t.d4_rid AND t3.qid = t2.d32_qid
    LEFT JOIN {dict13_stage} t4
        ON t3.country1_id = t4.rid
    WHERE t2.d32_enabled = 1
      AND t2.d32_mode = 0
      AND t2.d32_qid > 0
      AND toYear(t3.created) != 1970
    """

    sql_7 = f"""
    INSERT INTO {d7}
    SELECT
        t3.created                  AS created,
        toYear(t3.created)          AS year,
        toMonth(t3.created)         AS month,
        t2.d32_qid                  AS qid,
        t.d3_orgname                AS orgname,
        t3.airport_start            AS airport,
        t5.town                     AS city,
        t7.country                  AS country
    FROM {dict3_flat} t
    JOIN {dict31_flat} t2
        ON t.d4_rid = t2.d31_operatorid
    LEFT JOIN {dict90_flat} t3
        ON t3.tid = t.d4_rid AND t3.qid = t2.d32_qid
    LEFT JOIN {dict59_stage} t4
        ON t4.iata = t3.airport_start
    LEFT JOIN {dict15_stage} t5
        ON t5.rid = t4.bindrid
    LEFT JOIN {dict14_stage} t6
        ON t6.rid = t5.bindrid
    LEFT JOIN {dict13_stage} t7
        ON t7.rid = t6.bindrid
    WHERE t2.d32_enabled = 1
      AND t2.d32_mode = 0
      AND t2.d32_qid > 0
      AND toYear(t3.created) != 1970
    """

    sql_9 = f"""
    INSERT INTO {d9}
    SELECT
        t3.created                  AS created,
        toYear(t3.created)          AS year,
        toMonth(t3.created)         AS month,
        case
            when toMonth(t3.created)=1 then 'Январь'
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
        end                         AS month_russian,
        t2.d32_qid                  AS qid,
        t.d3_orgname                AS orgname,
        t3.airport_start            AS airport,
        t7.country                  AS country,
        t5.town                     AS city
    FROM {dict3_flat} t
    JOIN {dict31_flat} t2
        ON t.d4_rid = t2.d31_operatorid
    LEFT JOIN {dict90_flat} t3
        ON t3.tid = t.d4_rid AND t3.qid = t2.d32_qid
    LEFT JOIN {dict59_stage} t4
        ON t4.iata = t3.airport_start
    LEFT JOIN {dict15_stage} t5
        ON t5.rid = t4.bindrid
    LEFT JOIN {dict14_stage} t6
        ON t6.rid = t5.bindrid
    LEFT JOIN {dict13_stage} t7
        ON t7.rid = t6.bindrid
    WHERE t2.d32_enabled = 1
      AND t2.d32_mode = 0
      AND t2.d32_qid > 0
      AND toYear(t3.created) != 1970
    """

    sql_11 = f"""
    INSERT INTO {d11}
    SELECT
        t3.created                  AS created,
        toYear(t3.created)          AS year,
        toMonth(t3.created)         AS month,
        case
            when toMonth(t3.created)=1 then 'Январь'
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
        end                         AS month_russian,
        t.d3_orgname                AS orgname,
        t2.d32_qid                  AS qid
    FROM {dict3_flat} t
    JOIN {dict31_flat} t2
        ON t.d4_rid = t2.d31_operatorid
    LEFT JOIN {dict90_flat} t3
        ON t3.tid = t.d4_rid AND t3.qid = t2.d32_qid
    WHERE t2.d32_enabled = 1
      AND t2.d32_mode = 0
      AND t2.d32_qid > 0
      AND toYear(t3.created) != 1970
    """

    sql_12 = f"""
    INSERT INTO {d12}
    SELECT
        t3.created                           AS created,
        toYear(t3.created)                   AS year,
        t.d3_orgname                         AS orgname,
        t.d4_allow_auto_tour                 AS allow_auto_tour,
        case
            when t.d4_allow_auto_tour = 1 then 'Да'
            else 'Нет'
        end                                  AS allow_auto_tour_russian,
        t3.from_cabinet                      AS from_cabinet,
        t.d4_list                            AS list,
        case
            when t.d4_list=0 then 'Туроператоры'
            when t.d4_list=1 then 'Фрахтователи'
            when t.d4_list=2 then 'Вышедшие туроператоры'
            when t.d4_list=3 then 'Приостановившиеся деятельность'
            when t.d4_list=4 then 'Скрытые'
            else null
        end                                  AS list_russian,
        t3.passport                          AS passport,
        t2.d32_qid                           AS qid
    FROM {dict3_flat} t
    JOIN {dict31_flat} t2
        ON t.d4_rid = t2.d31_operatorid
    LEFT JOIN {dict90_flat} t3
        ON t3.tid = t.d4_rid AND t3.qid = t2.d32_qid
    WHERE t2.d32_enabled = 1
      AND t2.d32_mode = 0
      AND t2.d32_qid > 0
      AND toYear(t3.created) != 1970
    """

    sql_19 = f"""
    INSERT INTO {d19}
    SELECT
        t3.created                           AS created,
        toYear(t3.created)                   AS year,
        toMonth(t3.created)                  AS month,
        case
            when toMonth(t3.created)=1  then 'Январь'
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
        end                                  AS month_russian,
        t.d3_orgname                         AS orgname,
        t2.d32_qid                           AS qid,
        t4.country                           AS country
    FROM {dict3_flat} t
    JOIN {dict31_flat} t2
        ON t.d4_rid = t2.d31_operatorid
    LEFT JOIN {dict90_flat} t3
        ON t3.tid = t.d4_rid AND t3.qid = t2.d32_qid
    LEFT JOIN {dict13_stage} t4
        ON t3.country1_id = t4.rid
    WHERE t2.d32_enabled = 1
      AND t2.d32_mode = 0
      AND t2.d32_qid > 0
      AND toYear(t3.created) != 1970
    """

    return {
        "t_so_dashboard_5": sql_5,
        "t_so_dashboard_7": sql_7,
        "t_so_dashboard_9": sql_9,
        "t_so_dashboard_11": sql_11,
        "t_so_dashboard_12": sql_12,
        "t_so_dashboard_19": sql_19,
    }


def refresh_one_dashboard(table: str):
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


# =========================
# DAG
# =========================
default_args = {
    "owner": "serzhan",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=50),
}

with DAG(
    dag_id="mysql_clickhouse_increment_stage_flat_dashboards",
    start_date=datetime(2024, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=55),
    tags=["incremental", "mysql", "clickhouse", "raw", "stage", "flat", "dashboards"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # RAW INCREMENTAL LOAD
    # =========================
    with TaskGroup(group_id="dicts_basic_incremental") as g_basic:
        t13 = PythonOperator(
            task_id="load_dict13",
            python_callable=incremental_load_table,
            op_kwargs={"table_name": "dict13", "raw_table": "dict13_raw"},
        )
        t14 = PythonOperator(
            task_id="load_dict14",
            python_callable=incremental_load_table,
            op_kwargs={"table_name": "dict14", "raw_table": "dict14_raw"},
        )
        t15 = PythonOperator(
            task_id="load_dict15",
            python_callable=incremental_load_table,
            op_kwargs={"table_name": "dict15", "raw_table": "dict15_raw"},
        )
        t59 = PythonOperator(
            task_id="load_dict59",
            python_callable=incremental_load_table,
            op_kwargs={"table_name": "dict59", "raw_table": "dict59_raw"},
        )
        t13 >> t14 >> t15 >> t59

    with TaskGroup(group_id="dict3_4_incremental") as g_34:
        t3 = PythonOperator(
            task_id="load_dict3",
            python_callable=incremental_load_table,
            op_kwargs={"table_name": "dict3", "raw_table": "dict3_raw"},
        )
        t4 = PythonOperator(
            task_id="load_dict4",
            python_callable=incremental_load_table,
            op_kwargs={"table_name": "dict4", "raw_table": "dict4_raw"},
        )
        t3 >> t4

    with TaskGroup(group_id="dict31_32_incremental") as g_3132:
        t31 = PythonOperator(
            task_id="load_dict31",
            python_callable=incremental_load_table,
            op_kwargs={"table_name": "dict31", "raw_table": "dict31_raw"},
        )
        t32 = PythonOperator(
            task_id="load_dict32",
            python_callable=incremental_load_table,
            op_kwargs={"table_name": "dict32", "raw_table": "dict32_raw"},
        )
        t31 >> t32

    with TaskGroup(group_id="dict90_91_incremental") as g_9091:
        t90 = PythonOperator(
            task_id="load_dict90",
            python_callable=incremental_load_table,
            op_kwargs={"table_name": "dict90", "raw_table": "dict90_raw"},
        )
        t91 = PythonOperator(
            task_id="load_dict91",
            python_callable=incremental_load_table,
            op_kwargs={"table_name": "dict91", "raw_table": "dict91_raw"},
        )
        t90 >> t91

    # =========================
    # FLAT BUILD FROM STAGE VIEWS
    # =========================
    with TaskGroup(group_id="build_flats_from_stage") as g_flats:
        t3flat = PythonOperator(
            task_id="build_dict3_flat_from_stage",
            python_callable=build_dict3_flat_from_stage,
        )
        t31flat = PythonOperator(
            task_id="build_dict31_flat_from_stage",
            python_callable=build_dict31_flat_from_stage,
        )
        t90flat = PythonOperator(
            task_id="build_dict90_flat_from_stage",
            python_callable=build_dict90_flat_from_stage,
        )

    # =========================
    # DASHBOARDS
    # =========================
    with TaskGroup(group_id="refresh_dashboards") as g_dash:
        d5 = PythonOperator(
            task_id="refresh_dashboard_5",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_5"},
        )
        d7 = PythonOperator(
            task_id="refresh_dashboard_7",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_7"},
        )
        d9 = PythonOperator(
            task_id="refresh_dashboard_9",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_9"},
        )
        d11 = PythonOperator(
            task_id="refresh_dashboard_11",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_11"},
        )
        d12 = PythonOperator(
            task_id="refresh_dashboard_12",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_12"},
        )
        d19 = PythonOperator(
            task_id="refresh_dashboard_19",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_19"},
        )

        d5 >> d7 >> d9 >> d11 >> d12 >> d19

    start >> [g_basic, g_34, g_3132, g_9091]
    [g_basic, g_34, g_3132, g_9091] >> g_flats
    g_flats >> g_dash >> end
