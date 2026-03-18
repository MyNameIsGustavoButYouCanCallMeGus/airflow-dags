import os
###final update7
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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    res = ch_client.query(f"describe table {raw_table}")
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
        select 
                t.last_changed
        from etl_watermarks t
        where 1=1
          and t.table_name = '{table_name}'
        order by t.updated_at desc
        limit 1
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
        insert into etl_watermarks (table_name, last_changed)
        values ('{table_name}', toDateTime('{max_changed:%Y-%m-%d %H:%M:%S}'))
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
                select 
                        t.*
                from {table_name} t
                where 1=1
                  and t.changed >= %s
                order by t.changed, 
                         t.rid
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
    insert into `{ch_db}`.`dict31_flat`
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
    from `{ch_db}`.`dict31_stage` t
    left join `{ch_db}`.`dict32_stage` t2 on t.rid = t2.bindrid
    """

    t0 = time.time()
    ch.command(sql)
    cnt = ch.query(f"SELECT count() FROM `{ch_db}`.`dict31_flat`").result_rows[0][0]
    print(f"=== DONE BUILD dict31_flat | rows={cnt} | {time.time()-t0:.2f}s ===")


def build_dict3_flat_from_stage():
    ch_db, ch = _ch_client()
    print(f"=== BUILD {ch_db}.dict3_flat FROM STAGE VIEWS ===")

    ch.command(f"TRUNCATE TABLE {ch_db}.dict3_flat")

    sql = f"""
    insert into {ch_db}.dict3_flat (
        d3_rid,
        d3_changed,
        d3_user,
        d3_enabled,
        d3_orgname,
        d3_orgtype,
        d3_orgdate,
        d3_country,
        d3_town,
        d3_address,
        d3_address2,
        d3_phone,
        d3_email,
        d3_site,
        d3_bankinfo,
        d3_member,
        d3_iik,
        d3_bik,
        d3_bin,
        d3_kbe,
        d4_rid,
        d4_changed,
        d4_user,
        d4_enabled,
        d4_bindrid,
        d4_commission,
        d4_guarantee,
        d4_guarantee_num,
        d4_guarantee_date,
        d4_chieffname,
        d4_agreement,
        d4_created,
        d4_status,
        d4_about,
        d4_tourfirmname,
        d4_filials,
        d4_licence,
        d4_founders,
        d4_insurance,
        d4_offices,
        d4_cellphone,
        d4_bad_past_tour,
        d4_create_past_tour,
        d4_no_create_tour,
        d4_allow_auto_tour,
        d4_list,
        d4_is_agent,
        d4_remarks,
        d4_auto_bad_tour,
        d4_hajj,
        d4_description
    )
    select
            t.rid                                                                       as d3_rid,
            t.changed                                                                   as d3_changed,
            t.user                                                                      as d3_user,
            t.enabled                                                                   as d3_enabled,
            t.orgname                                                                   as d3_orgname,
            t.orgtype                                                                   as d3_orgtype,
            ifNull(CAST(t.orgdate as Nullable(Date)), toDate('1970-01-01'))             as d3_orgdate,
            t.country                                                                   as d3_country,
            t.town                                                                      as d3_town,
            t.address                                                                   as d3_address,
            t.address2                                                                  as d3_address2,
            t.phone                                                                     as d3_phone,
            t.email                                                                     as d3_email,
            t.site                                                                      as d3_site,
            t.bankinfo                                                                  as d3_bankinfo,
            t.member                                                                    as d3_member,
            t.iik                                                                       as d3_iik,
            t.bik                                                                       as d3_bik,
            t.bin                                                                       as d3_bin,
            t.kbe                                                                       as d3_kbe,
            t2.rid                                                                      as d4_rid,
            t2.changed                                                                  as d4_changed,
            t2.user                                                                     as d4_user,
            t2.enabled                                                                  as d4_enabled,
            t2.bindrid                                                                  as d4_bindrid,
            t2.commission                                                               as d4_commission,
            t2.guarantee                                                                as d4_guarantee,
            t2.guarantee_num                                                            as d4_guarantee_num,
            ifNull(CAST(t2.guarantee_date as Nullable(Date)), toDate('1970-01-01'))     as d4_guarantee_date,
            t2.chieffname                                                               as d4_chieffname,
            t2.agreement                                                                as d4_agreement,
            CAST(t2.created as Nullable(Date))                                          as d4_created,
            t2.status                                                                   as d4_status,
            t2.about                                                                    as d4_about,
            t2.tourfirmname                                                             as d4_tourfirmname,
            t2.filials                                                                  as d4_filials,
            t2.licence                                                                  as d4_licence,
            t2.founders                                                                 as d4_founders,
            t2.insurance                                                                as d4_insurance,
            t2.offices                                                                  as d4_offices,
            t2.cellphone                                                                as d4_cellphone,
            t2.bad_past_tour                                                            as d4_bad_past_tour,
            t2.create_past_tour                                                         as d4_create_past_tour,
            t2.no_create_tour                                                           as d4_no_create_tour,
            t2.allow_auto_tour                                                          as d4_allow_auto_tour,
            t2.list                                                                     as d4_list,
            t2.is_agent                                                                 as d4_is_agent,
            t2.remarks                                                                  as d4_remarks,
            t2.auto_bad_tour                                                            as d4_auto_bad_tour,
            ifNull(CAST(t2.hajj as Nullable(Date)), toDate('1970-01-01'))               as d4_hajj,
            t2.description                                                              as d4_description
    from {ch_db}.dict3_stage t
    left join {ch_db}.dict4_stage t2 on t.rid = t2.bindrid
    """

    print("===== SQL START =====")
    print(sql)
    print("===== SQL END =====")

    ch.command(sql)

    cnt = ch.query(f"select count() from {ch_db}.dict3_flat").result_rows[0][0]
    print(f"=== DONE BUILD dict3_flat | rows={cnt} ===")

def build_dict90_flat_from_stage():
    ch_db, ch = _ch_client()

    print(f"=== BUILD `{ch_db}`.`dict90_flat` FROM STAGE VIEWS ===")
    ch.command(f"truncate table `{ch_db}`.`dict90_flat`")

    sql = f"""
    insert into `{ch_db}`.`dict90_flat`
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
    from `{ch_db}`.`dict90_stage` d
    left join `{ch_db}`.`dict91_stage` d2 on d.rid = d2.bindrid
    """

    t0 = time.time()
    ch.command(sql)
    cnt = ch.query(f"select count() from `{ch_db}`.`dict90_flat`").result_rows[0][0]
    print(f"=== DONE BUILD dict90_flat | rows={cnt} | {time.time()-t0:.2f}s ===")


# =========================
# DASHBOARD REFRESH
# =========================
def _dashboard_inserts(ch_db: str):
    d1  = f"`{ch_db}`. `t_so_dashboard_1`"
    d2  = f"`{ch_db}`.`t_so_dashboard_2`"
    d3  = f"`{ch_db}`.`t_so_dashboard_3`"
    d4  = f"`{ch_db}`.`t_so_dashboard_4`"
    d5  = f"`{ch_db}`.`t_so_dashboard_5`"
    d6  = f"`{ch_db}`.`t_so_dashboard_6`"
    d7  = f"`{ch_db}`.`t_so_dashboard_7`"
    d8  = f"`{ch_db}`.`t_so_dashboard_8`"
    d9  = f"`{ch_db}`.`t_so_dashboard_9`"
    d10 = f"`{ch_db}`.`t_so_dashboard_10`"
    d11 = f"`{ch_db}`.`t_so_dashboard_11`"
    d12 = f"`{ch_db}`.`t_so_dashboard_12`"
    d13 = f"`{ch_db}`.`t_so_dashboard_13`"
    d14 = f"`{ch_db}`.`t_so_dashboard_14`"

    dict3_flat = f"`{ch_db}`.`dict3_flat`"
    dict31_flat = f"`{ch_db}`.`dict31_flat`"
    dict90_flat = f"`{ch_db}`.`dict90_flat`"

    dict13_stage = f"`{ch_db}`.`dict13_stage`"
    dict14_stage = f"`{ch_db}`.`dict14_stage`"
    dict15_stage = f"`{ch_db}`.`dict15_stage`"
    dict59_stage = f"`{ch_db}`.`dict59_stage`"

    sql_1 = f"""
    insert into {d1}
    with base as (
        select
    	        t.qid 											as qid,
    	        toDate(t.date_start) 							as date_start,
    	        toDate(t.date_end) 								as date_end,
    	        arrayJoin(
    	            arrayFilter(x -> x != 0, [
    	                toUInt32(ifNull(t.country1_id, 0)),
    	                toUInt32(ifNull(t.country2_id, 0)),
    	                toUInt32(ifNull(t.country3_id, 0)),
    	                toUInt32(ifNull(t.country4_id, 0)),
    	                toUInt32(ifNull(t.country5_id, 0)),
    	                toUInt32(ifNull(t.country6_id, 0))
    	            ])
    	        ) 												as country_id
        from {dict90_flat} t
        where 1 = 1
          and ifNull(t.enabled, 0) = 1
          and t.qid is not null
          and t.date_start is not null
          and t.date_end is not null
          and toDate(t.date_end) >= toDate(t.date_start)
    )
    select
    	    today() 													as as_of_date,
    	    formatDateTime(today(), '%d.%m.%Y') 						as as_of_date_ru,
    	    b.country_id 												as country_id,
    	    ifNull(c.country, '') 										as country_ru,
    	    ifNull(c.countryen, '') 									as country_en,
    	    ifNull(c.country_code, '') 									as country_code,
    	    countDistinctIf(
    	        b.qid,
    	        b.date_start >= addMonths(toStartOfMonth(today()), -1)
    	        and b.date_start < toStartOfMonth(today())
    	    ) 															as departed_prev_month_cnt,
    	    countDistinctIf(
    	        b.qid,
    	        b.date_start >= toStartOfMonth(today())
    	        and b.date_start <= today()
    	    ) 															as departed_curr_month_cnt,
    	    countDistinctIf(
    	        b.qid,
    	        b.date_start <= today()
    	        and b.date_end > today()
    	    ) 															as today_there_cnt,
    	    countDistinctIf(
    	        b.qid,
    	        b.date_end = today()
    	    ) 															as return_today_cnt,
    	    countDistinctIf(
    	        b.qid,
    	        b.date_end = addDays(today(), 1)
    	    ) 															as return_tomorrow_cnt,
    	    countDistinctIf(
    	        b.qid,
    	        b.date_end = addDays(today(), 2)
    	    ) 															as return_after_tomorrow_cnt
    from base b
    left join {dict13_stage} c on c.rid = b.country_id
       									and ifNull(c.enabled, 0) = 1
    where 1 = 1
      and ifNull(c.country_code, '') != ''
    group by
        b.country_id,
        c.country,
        c.countryen,
        c.country_code
    """

    sql_2 = f"""
    insert into {d2}
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
    	    t.d3_orgname				as orgname,
    		t3.airport_start			as airport,
    		t7.country					as country,
    		t5.town						as city
    from {dict3_flat} t
    join {dict31_flat} t2          on t.d4_rid = t2.d31_operatorid
    left join {dict90_flat} t3     on t3.tid = t.d4_rid and t3.qid = t2.d32_qid
    left join {dict59_stage} t4    on t4.iata = t3.airport_start
    left join {dict15_stage} t5    on t5.rid = t4.bindrid
    left join {dict14_stage} t6    on t6.rid = t5.bindrid
    left join {dict13_stage} t7    on t7.rid = t6.bindrid
    where 1=1
      and t2.d32_enabled=1
      and t2.d32_mode=0
      and t2.d32_qid>0
      and toYear(t3.created)!=1970
    """

    sql_3 = f"""
    insert into {d3}
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
    join {dict31_flat} t2 	 	   on t.d4_rid=t2.d31_operatorid
    left join {dict90_flat} t3     on t3.tid = t.d4_rid and t3.qid = t2.d32_qid
    left join {dict59_stage} t4    on t4.iata = t3.airport_start
    left join {dict15_stage} t5    on t5.rid = t4.bindrid
    left join {dict14_stage} t6    on t6.rid = t5.bindrid
    left join {dict13_stage} t7    on t7.rid = t6.bindrid
    where 1=1
      and t2.d32_enabled=1
      and t2.d32_mode=0
      and t2.d32_qid>0
      and toYear(t3.created)!=1970
    order by t3.created desc
    """

    sql_4 = f"""
    insert into {d4}
    select
    	    t3.created              as created,
    	    t3.number               as tourcode,
    	    concat(
    	        'https://report.fondkamkor.kz/Voucher/queries/',
    	        toString(t3.number),
    	        '/view'
    	    )                       as tourcode_url,
    	    t.d3_orgname            as touragent,
    	    t3.qid                  as qid,
    	    t3.date_start           as date_start,
    	    t3.date_end             as date_end,
    	    t3.airlines             as airlines,
    	    t3.airport_start        as airport_kz,
    	    t3.airport_end          as airport_dest,
    	    t4.country              as country,
    	    t3.passport             as passport,
    	    t.d4_description        as note,
    	    t3.sub_date_start       as sub_date_start,
    	    t3.sub_date_end         as sub_date_end,
    	    t3.sub_airlines         as sub_airlines,
    	    t3.sub_airport          as sub_airport
    from {dict3_flat} t
    join {dict31_flat} t2       on t.d4_rid = t2.d31_operatorid
    left join {dict90_flat} t3  on t3.tid = t.d4_rid and t3.qid = t2.d32_qid
    left join {dict13_stage} t4 on t3.country1_id = t4.rid
    where 1=1
      and t2.d32_enabled = 1
      and t2.d32_mode = 0
      and t2.d32_qid > 0
      and toYear(t3.created) != 1970
    """

    sql_5 = f"""
    insert into {d5}
    with op_flat as (
        select
    	        toUInt32(ifNull(t.d4_rid, 0)) 							    as operator_id,
    	        argMax(ifNull(t.d4_tourfirmname, 'Нет данных'), d4_changed) as operator_name
        from {dict3_flat} t
        where 1=1
          and ifNull(t.d4_enabled, 0) = 1
          and ifNull(t.d4_is_agent, 0) = 0
          and t.d4_rid is not null
        group by 1
    ),
    base as (
        select
    	        toUInt64(d90.qid) 				as tour_id,
    	        toDate(d90.date_start) 			as date_start,
    	        toDate(d90.date_end)   			as date_end,
    	        nullIf(d90.airport_end, '')  	as airport_iata,
    	        toUInt32(ifNull(d90.tid, 0)) 	as touroperator_id,
    	        toUInt32(country_rid) 			as country_id
        from {dict90_flat} d90
        array join arrayFilter(
            x -> x != 0,
            [
                toUInt32(ifNull(d90.country1_id, 0)),
                toUInt32(ifNull(d90.country2_id, 0)),
                toUInt32(ifNull(d90.country3_id, 0)),
                toUInt32(ifNull(d90.country4_id, 0)),
                toUInt32(ifNull(d90.country5_id, 0)),
                toUInt32(ifNull(d90.country6_id, 0))
            ]
        ) as country_rid
        where 1=1
          and ifNull(d90.enabled, 0) = 1
          and d90.qid is not null
          and d90.date_start is not null
          and d90.date_end is not null
          and toDate(d90.date_end) >= toDate(d90.date_start)
    ),
    expanded as (
        select
    	        tour_id					as tour_id,
    	        country_id				as country_id,
    	        airport_iata			as airport_iata,
    	        touroperator_id			as touroperator_id,
    	        addDays(date_start, n)  as as_of_date,
    	        date_start				as date_start,
    	        date_end				as date_end
        from base
        array join range(dateDiff('day', date_start, date_end) + 1) as n
    )
    select
    	    e.as_of_date											as as_of_date,
    	    formatDateTime(e.as_of_date, '%d.%m.%Y') 				as as_of_date_ru,
    	    e.country_id											as country_id,
    	    ifNull(c.country, '')      								as country_ru,
    	    ifNull(c.countryen, '')    								as country_en,
    	    ifNull(c.country_code, '') 								as country_code,
    	    ifNull(e.airport_iata, '') 								as airport_iata,
    	    ifNull(a.airport, 'Нет данных')    						as airport_ru,
    	    ifNull(a.airport_en, 'Нет данных') 						as airport_en,
    	    e.touroperator_id										as touroperator_id,
    	    ifNull(op.operator_name, 'Нет данных') 					as touroperator_name,
    	    countDistinctIf(e.tour_id, e.as_of_date = e.date_start) as arrived_cnt,
    	    countDistinctIf(e.tour_id, e.as_of_date = e.date_end)   as departing_cnt,
    	    countDistinct(e.tour_id) 								as in_country_cnt,
    	    greatest(
    	        toUInt64(0),
    	        countDistinct(e.tour_id)
    	        - countDistinctIf(e.tour_id, e.as_of_date = e.date_start)
    	        - countDistinctIf(e.tour_id, e.as_of_date = e.date_end)
    	    ) 														as staying_only_cnt,
    	    countDistinct(e.tour_id) 								as total_on_date_cnt
    from expanded e
    left join {dict13_stage} c on c.rid = e.country_id
       									and ifNull(c.enabled, 0) = 1
    left join {dict59_stage} a on a.iata = e.airport_iata
       									and ifNull(a.enabled, 0) = 1
    left join op_flat op 				on op.operator_id = e.touroperator_id
    where 1=1
      and ifNull(c.country_code, '') != ''
    group by
        e.as_of_date,
        e.country_id,
        c.country,
        c.countryen,
        c.country_code,
        e.airport_iata,
        a.airport,
        a.airport_en,
        e.touroperator_id,
        op.operator_name
    """

    sql_6 = f"""
    insert into {d6}
    select
            t3.created                           as created,
            toYear(t3.created)                   as year,
            toMonth(t3.created)                  as month,
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
            end                                  as month_russian,
            t.d3_orgname                         as orgname,
            t2.d32_qid                           as qid,
            t4.country                           as country
    from {dict3_flat} t
    join {dict31_flat} t2 on t.d4_rid = t2.d31_operatorid
    left join {dict90_flat} t3 on t3.tid = t.d4_rid and t3.qid = t2.d32_qid
    left join {dict13_stage} t4 on t3.country1_id = t4.rid
    where 1=1
      and t2.d32_enabled = 1
      and t2.d32_mode = 0
      and t2.d32_qid > 0
      and toYear(t3.created) != 1970
    """

    sql_7 = f"""
    insert into {d7}
    select
    	    case when t.touragent_bin = '' or t.touragent_bin is null then 'Неизвестный БИН'
    	         else trim(t.touragent_bin)
    	    end 				as bin,
    	    case when t2.d4_tourfirmname is null or t2.d4_tourfirmname='' then 'Неизвестные'
    	         else trim(t2.d4_tourfirmname) 
    	    end 				as orgname,
    	    t.qid 				as qid,
    	    t.created 			as created,
    	    toYear(t.created) 	as year,
    	    toMonth(t.created) 	as month
    from {dict90_flat} t
    left join {dict3_flat} t2 on t.touragent_bin = t2.d3_bin
    order by t.qid desc
    """

    sql_8 = f"""
    insert into {d8}
    with agents as (
        select
    	        toUInt32(t.d3_rid) 						as agent_rid,
    	        nullIf(trim(toString(t.d3_bin)), '') 	as iin_bin,
    	        argMax(nullIf(trim(t.d3_orgname), ''),
    	            ifNull(t.d4_changed, t.d3_changed)
    	        	) 									as company_name,
    	        argMax(nullIf(trim(t.d3_town), ''),
    	            ifNull(t.d4_changed, t.d3_changed)
    	        	) 									as company_city,
    	        argMax(nullIf(trim(t.d3_site), ''),
    	            ifNull(t.d4_changed, t.d3_changed)
    	        	) 									as site,
    	        nullIf(argMax(
    	                toUInt32(ifNull(t.d4_rid, 0)),
    	                ifNull(t.d4_changed, t.d3_changed)
    	            ),0) 						 		as agent_dict4_id,
    	        cast(argMax(
    	                ifNull(t.d4_status, null),
    	                ifNull(t.d4_changed, t.d3_changed)
    	            ),'Nullable(UInt8)') 				as login_cabinet_flag
        from fondkamkor.dict3_flat t
        where 1=1
          and ifNull(t.d3_enabled, 0)=1
          and (isNull(t.d4_is_agent) or t.d4_is_agent=1)
          and nullIf(trim(toString(t.d3_bin)), '') is not null
        group by
            t.d3_rid,
            t.d3_bin
    ),
    agent_city as (
        select
    	        toUInt32(t2.bindrid) 	as bindrid,
    	        argMax(nullIf(trim(t2.town), ''),
    	            changed
    	        ) 						as city
        from fondkamkor.dict15_stage t2
        where 1=1
          and t2.enabled = 1
        group by t2.bindrid
    ),
    base as (
        select
    	        toUInt64(t.qid) 							as qid,
    	        toUInt32(t.rid) 							as tour_rid,
    	        toString(t.number) 							as tourcode_number,
    	        toDateTime(t.created) 						as created_dt,
    	        toDate(t.created) 							as created_date,
    	        toStartOfMonth(toDate(t.created)) 			as month_start,
    	        toUInt16(toYear(t.created)) 				as year,
    	        toUInt8(toMonth(t.created)) 				as month_num,
    	        multiIf(
    	            toMonth(t.created) = 1,  'Январь',
    	            toMonth(t.created) = 2,  'Февраль',
    	            toMonth(t.created) = 3,  'Март',
    	            toMonth(t.created) = 4,  'Апрель',
    	            toMonth(t.created) = 5,  'Май',
    	            toMonth(t.created) = 6,  'Июнь',
    	            toMonth(t.created) = 7,  'Июль',
    	            toMonth(t.created) = 8,  'Август',
    	            toMonth(t.created) = 9,  'Сентябрь',
    	            toMonth(t.created) = 10, 'Октябрь',
    	            toMonth(t.created) = 11, 'Ноябрь',
    	            'Декабрь'
    	        ) as month_ru,
    	        nullIf(trim(toString(t.touragent_bin)), '') as iin_bin,
    	        a.company_name,
    	        coalesce(
    	            c.city,
    	            a.company_city
    	        ) 											as city,
    	        toUInt8(ifNull(t.from_cabinet, 0)) 			as from_cabinet_flag,
    	        if(toUInt8(ifNull(t.from_cabinet, 0))=1,
    	            'Ручной ввод',
    	            'Кабинет'
    	        ) 											as from_cabinet_label,
    	        a.agent_dict4_id,
    	        if(isNull(a.agent_dict4_id),
    	            cast(null, 'Nullable(String)'),
    	            concat(
    	                'https://report.fondkamkor.kz/Voucher/agent/',
    	                toString(a.agent_dict4_id),
    	                '/home'
    	            )
    	        ) 											as cabinet_url,
    	        if(isNull(a.agent_dict4_id),
    	            'Нет ссылки',
    	            'Кабинет'
    	        ) 											as cabinet_link_text,
    	        a.login_cabinet_flag,
    	        multiIf(
    	            a.login_cabinet_flag = 1, 'Да',
    	            a.login_cabinet_flag = 0, 'Нет',
    	            'Нет данных'
    	        ) 											as login_cabinet_label,
    	        toUInt8(1) 									as issued_cnt,
    	        toUInt8(rand() % 2) 						as confirmed
        from fondkamkor.dict90_flat t
        left join agents a 		on a.iin_bin = nullIf(trim(toString(t.touragent_bin)), '')
        left join agent_city c 	on c.bindrid = a.agent_rid
        where 1=1
          and ifNull(t.enabled, 0) = 1
    )
    select
    	    b.qid														as qid,
    	    b.tour_rid													as tour_rid,
    	    b.tourcode_number											as tourcode_number,
    	    b.created_dt												as created_dt,
    	    b.created_date												as created_date,
    	    b.month_start												as month_start,
    	    b.year														as year,
    	    b.month_num													as month_num,
    	    b.month_ru													as month_ru,
    	    b.iin_bin													as iin_bin,
    	    b.company_name												as company_name,
    	    b.city														as city,
    	    if(b.company_name is null, 'Нет данных', b.company_name) 	as company_name_label,
    	    if(b.city is null, 'Нет данных', b.city) 					as city_label,
    	    b.from_cabinet_flag											as from_cabinet_flag,
    	    b.from_cabinet_label										as from_cabinet_label,
    	    b.agent_dict4_id											as agent_dict4_id,
    	    b.cabinet_url												as cabinet_url,
    	    b.cabinet_link_text											as cabinet_link_text,
    	    b.login_cabinet_flag										as login_cabinet_flag,
    	    b.login_cabinet_label										as login_cabinet_label,
    	    b.issued_cnt												as issued_cnt,
    	    b.confirmed													as confirmed
    from base b
    """

    sql_9 = f"""
    insert into {d9}
    with agents as (
        select
    	        toUInt32(t.d3_rid) 						as agent_rid,
    	        nullIf(trim(toString(t.d3_bin)), '') 	as iin_bin,
    	        argMax(nullIf(trim(t.d3_orgname), ''),
    	            ifNull(t.d4_changed, t.d3_changed)
    	        	) 									as company_name,
    	        argMax(nullIf(trim(t.d3_town), ''),
    	            ifNull(t.d4_changed, t.d3_changed)
    	        	) 									as company_city,
    	        argMax(nullIf(trim(t.d3_site), ''),
    	            ifNull(t.d4_changed, t.d3_changed)
    	        	) 									as site,
    	        nullIf(argMax(
    	                toUInt32(ifNull(t.d4_rid, 0)),
    	                ifNull(t.d4_changed, t.d3_changed)
    	            ),0) 						 		as agent_dict4_id,
    	        cast(argMax(
    	                ifNull(t.d4_status, null),
    	                ifNull(t.d4_changed, t.d3_changed)
    	            ),'Nullable(UInt8)') 				as login_cabinet_flag
        from fondkamkor.dict3_flat t
        where 1=1
          and ifNull(t.d3_enabled, 0)=1
          and (isNull(t.d4_is_agent) or t.d4_is_agent=1)
          and nullIf(trim(toString(t.d3_bin)), '') is not null
        group by
            t.d3_rid,
            t.d3_bin
    ),
    agent_city as (
        select
    	        toUInt32(t2.bindrid) 	as bindrid,
    	        argMax(nullIf(trim(t2.town), ''),
    	            changed
    	        ) 						as city
        from fondkamkor.dict15_stage t2
        where 1=1
          and t2.enabled = 1
        group by t2.bindrid
    ),
    base as (
        select
    	        toUInt64(t.qid) 							as qid,
    	        toUInt32(t.rid) 							as tour_rid,
    	        toString(t.number) 							as tourcode_number,
    	        toDateTime(t.created) 						as created_dt,
    	        toDate(t.created) 							as created_date,
    	        toStartOfMonth(toDate(t.created)) 			as month_start,
    	        toUInt16(toYear(t.created)) 				as year,
    	        toUInt8(toMonth(t.created)) 				as month_num,
    	        multiIf(
    	            toMonth(t.created) = 1,  'Январь',
    	            toMonth(t.created) = 2,  'Февраль',
    	            toMonth(t.created) = 3,  'Март',
    	            toMonth(t.created) = 4,  'Апрель',
    	            toMonth(t.created) = 5,  'Май',
    	            toMonth(t.created) = 6,  'Июнь',
    	            toMonth(t.created) = 7,  'Июль',
    	            toMonth(t.created) = 8,  'Август',
    	            toMonth(t.created) = 9,  'Сентябрь',
    	            toMonth(t.created) = 10, 'Октябрь',
    	            toMonth(t.created) = 11, 'Ноябрь',
    	            'Декабрь'
    	        ) as month_ru,
    	        nullIf(trim(toString(t.touragent_bin)), '') as iin_bin,
    	        a.company_name,
    	        coalesce(
    	            c.city,
    	            a.company_city
    	        ) 											as city,
    	        toUInt8(ifNull(t.from_cabinet, 0)) 			as from_cabinet_flag,
    	        if(toUInt8(ifNull(t.from_cabinet, 0))=1,
    	            'Ручной ввод',
    	            'Кабинет'
    	        ) 											as from_cabinet_label,
    	        a.agent_dict4_id,
    	        if(isNull(a.agent_dict4_id),
    	            cast(null, 'Nullable(String)'),
    	            concat(
    	                'https://report.fondkamkor.kz/Voucher/agent/',
    	                toString(a.agent_dict4_id),
    	                '/home'
    	            )
    	        ) 											as cabinet_url,
    	        if(isNull(a.agent_dict4_id),
    	            'Нет ссылки',
    	            'Кабинет'
    	        ) 											as cabinet_link_text,
    	        a.login_cabinet_flag,
    	        multiIf(
    	            a.login_cabinet_flag = 1, 'Да',
    	            a.login_cabinet_flag = 0, 'Нет',
    	            'Нет данных'
    	        ) 											as login_cabinet_label,
    	        toUInt8(1) 									as issued_cnt
        from fondkamkor.dict90_flat t
        left join agents a 		on a.iin_bin = nullIf(trim(toString(t.touragent_bin)), '')
        left join agent_city c 	on c.bindrid = a.agent_rid
        where 1=1
          and ifNull(t.enabled, 0) = 1
    )
    select
    	    b.qid														as qid,
    	    b.tour_rid													as tour_rid,
    	    b.tourcode_number											as tourcode_number,
    	    b.created_dt												as created_dt,
    	    b.created_date												as created_date,
    	    b.month_start												as month_start,
    	    b.year														as year,
    	    b.month_num													as month_num,
    	    b.month_ru													as month_ru,
    	    b.iin_bin													as iin_bin,
    	    b.company_name												as company_name,
    	    b.city														as city,
    	    if(b.company_name is null, 'Нет данных', b.company_name) 	as company_name_label,
    	    if(b.city is null, 'Нет данных', b.city) 					as city_label,
    	    b.from_cabinet_flag											as from_cabinet_flag,
    	    b.from_cabinet_label										as from_cabinet_label,
    	    b.agent_dict4_id											as agent_dict4_id,
    	    b.cabinet_url												as cabinet_url,
    	    b.cabinet_link_text											as cabinet_link_text,
    	    b.login_cabinet_flag										as login_cabinet_flag,
    	    b.login_cabinet_label										as login_cabinet_label,
    	    b.issued_cnt												as issued_cnt
    from base b
    """
    
    sql_10 = f"""
    insert into {d10}
    select
            t3.created                  as created,
            toYear(t3.created)          as year,
            toMonth(t3.created)         as month,
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
            end                         as month_russian,
            t.d3_orgname                as orgname,
            t2.d32_qid                  as qid
    from {dict3_flat} t
    join {dict31_flat} t2      on t.d4_rid = t2.d31_operatorid
    left join {dict90_flat} t3 on t3.tid = t.d4_rid and t3.qid = t2.d32_qid
    where 1=1
      and t2.d32_enabled = 1
      and t2.d32_mode = 0
      and t2.d32_qid > 0
      and toYear(t3.created) != 1970
    """

    sql_11 = f"""
    insert into {d11}
    select
            t3.created                           as created,
            toYear(t3.created)                   as year,
            t.d3_orgname                         as orgname,
            t.d4_allow_auto_tour                 as allow_auto_tour,
            case when t.d4_allow_auto_tour = 1 then 'Да'
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
    where 1=1
      and t2.d32_enabled = 1
      and t2.d32_mode = 0
      and t2.d32_qid > 0
      and toYear(t3.created) != 1970
    """
    
    sql_12 = f"""
    insert into {d12}
    with agents as (
        select
    	        toUInt32(d3_rid) 					as agent_rid,
    	        nullIf(trim(toString(d3_bin)), '') 	as iin_bin,
    	        argMax(
    	            nullIf(trim(d3_orgname), ''),
    	            ifNull(d4_changed, d3_changed)
    	        ) 									as company_name,
    	        argMax(
    	            nullIf(trim(d3_town), ''),
    	            ifNull(d4_changed, d3_changed)
    	        ) 									as company_city
        from fondkamkor.dict3_flat
        where 1=1
          and ifNull(d3_enabled, 0) = 1
          and nullIf(trim(toString(d3_bin)), '') is not null
        group by
            d3_rid,
            d3_bin
    ),
    agent_city as (
        select
    	        toUInt32(bindrid) 	as bindrid,
    	        argMax(
    	            nullIf(trim(town), ''),
    	            changed
    	        ) 					as city
        from fondkamkor.dict15_stage
        where 1=1
          and enabled=1
        group by bindrid
    )
    select
        case when t.touragent_bin = '' or t.touragent_bin is null then 'Неизвестный БИН'
             else trim(t.touragent_bin)
        end 							as bin,
        case when a.company_name is null or a.company_name = '' then 'Неизвестные'
             else trim(a.company_name)
        end 							as orgname,
        case when coalesce(c.city, a.company_city) is null
                 or coalesce(c.city, a.company_city) = '' then 'Неизвестный город'
             else trim(coalesce(c.city, a.company_city))
        end 							as city,
        t.qid 							as qid,
        t.created 						as created,
        toYear(t.created) 				as year,
        toMonth(t.created) 				as month
    from fondkamkor.dict90_flat t
    left join agents a 		on a.iin_bin = nullIf(trim(toString(t.touragent_bin)), '')
    left join agent_city c 	on c.bindrid = a.agent_rid
    where 1=1
      and ifNull(t.enabled, 0) = 1
    """
    
    sql_13 = f"""
    insert into {d13}
    select
            t3.created                           as created,
            toYear(t3.created)                   as year,
            toMonth(t3.created)                  as month,
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
            end                                  as month_russian,
            t.d3_orgname                         as orgname,
            t2.d32_qid                           as qid,
            t4.country                           as country
    from {dict3_flat} t
    join {dict31_flat} t2 on t.d4_rid = t2.d31_operatorid
    left join {dict90_flat} t3 on t3.tid = t.d4_rid and t3.qid = t2.d32_qid
    left join {dict13_stage} t4 on t3.country1_id = t4.rid
    where 1=1
      and t2.d32_enabled = 1
      and t2.d32_mode = 0
      and t2.d32_qid > 0
      and toYear(t3.created) != 1970
    """

    sql_14 = f"""
    insert into {d14}
    select
            t3.created                           as created,
            toYear(t3.created)                   as year,
            toMonth(t3.created)                  as month,
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
            end                                  as month_russian,
            t.d3_orgname                         as orgname,
            t2.d32_qid                           as qid,
            t4.country                           as country
    from {dict3_flat} t
    join {dict31_flat} t2 on t.d4_rid = t2.d31_operatorid
    left join {dict90_flat} t3 on t3.tid = t.d4_rid and t3.qid = t2.d32_qid
    left join {dict13_stage} t4 on t3.country1_id = t4.rid
    where 1=1
      and t2.d32_enabled = 1
      and t2.d32_mode = 0
      and t2.d32_qid > 0
      and toYear(t3.created) != 1970
    """
    
    return {
        "t_so_dashboard_1":  sql_1,
        "t_so_dashboard_2":  sql_2,
        "t_so_dashboard_3":  sql_3,
        "t_so_dashboard_4":  sql_4,
        "t_so_dashboard_5":  sql_5,
        "t_so_dashboard_6":  sql_6,
        "t_so_dashboard_7":  sql_7,
        "t_so_dashboard_8":  sql_8,
        "t_so_dashboard_9":  sql_9,
        "t_so_dashboard_10": sql_10,
        "t_so_dashboard_11": sql_11,
        "t_so_dashboard_12":  sql_12,
        "t_so_dashboard_13": sql_13,
        "t_so_dashboard_14": sql_14
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
    dag_id="refresh_dashboards_mysql_superset_serzhan",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=55),
    tags=["incremental", "mysql", "clickhouse", "raw", "stage", "flat", "dashboards"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    trigger_self = TriggerDagRunOperator(
        task_id="trigger_self",
        trigger_dag_id="mysql_clickhouse_increment_stage_flat_dashboards_self_trigger",
    )

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
        d1 = PythonOperator(
            task_id="refresh_dashboard_1",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_1"},
        )
        d2 = PythonOperator(
            task_id="refresh_dashboard_2",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_2"},
        )
        d3 = PythonOperator(
            task_id="refresh_dashboard_3",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_3"},
        )
        d4 = PythonOperator(
            task_id="refresh_dashboard_4",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_4"},
        )
        d5 = PythonOperator(
            task_id="refresh_dashboard_5",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_5"},
        )
        d6 = PythonOperator(
            task_id="refresh_dashboard_6",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_6"},
        )
        d7 = PythonOperator(
            task_id="refresh_dashboard_7",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_7"},
        )
        d8 = PythonOperator(
            task_id="refresh_dashboard_8",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_8"},
        )
        d9 = PythonOperator(
            task_id="refresh_dashboard_9",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_9"},
        )
        d10 = PythonOperator(
            task_id="refresh_dashboard_10",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_10"},
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
        d13 = PythonOperator(
            task_id="refresh_dashboard_13",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_13"},
        )
        d14 = PythonOperator(
            task_id="refresh_dashboard_14",
            python_callable=refresh_one_dashboard,
            op_kwargs={"table": "t_so_dashboard_14"},
        )

        d1 >> d2 >> d3 >> d4 >> d5 >> d6 >> d7 >> d8 >> d9 >> d10 >> d11 >> d12 >> d13 >> d14

    start >> [g_basic, g_34, g_3132, g_9091]
    [g_basic, g_34, g_3132, g_9091] >> g_flats
    g_flats >> g_dash >> end >> trigger_self
