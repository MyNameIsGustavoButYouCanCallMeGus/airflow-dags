from __future__ import annotations

import re
from zoneinfo import ZoneInfo
from datetime import datetime, date, time as dtime, timedelta

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

OVERLAP_MINUTES = 5
DEBUG_SAMPLE_ROWS = 20

MYSQL_TZ = "Asia/Almaty"
TARGET_TZ = "UTC"

FALLBACK_DATE = date(1970, 1, 1)
FALLBACK_DATETIME = datetime(1970, 1, 1, 0, 0, 0)


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
    """
    Nullable(Date) -> Date
    Nullable(DateTime) -> DateTime
    LowCardinality(String) -> String
    Nullable(LowCardinality(Date)) -> Date
    """
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


def _local_kz_to_utc(dt_value: datetime | None) -> datetime | None:
    if dt_value is None:
        return None

    if dt_value.tzinfo is None:
        dt_value = dt_value.replace(tzinfo=ZoneInfo(MYSQL_TZ))

    return dt_value.astimezone(ZoneInfo(TARGET_TZ)).replace(tzinfo=None)


# =========================
# CLICKHOUSE SCHEMA HELPERS
# =========================
def _get_ch_column_meta(ch_client, raw_table: str) -> dict[str, dict]:
    """
    Returns:
    {
        "changed": {
            "raw_type": "DateTime",
            "base_type": "DateTime",
            "nullable": False,
        },
        "orgdate": {
            "raw_type": "Nullable(Date)",
            "base_type": "Date",
            "nullable": True,
        }
    }
    """
    res = ch_client.query(f"DESCRIBE TABLE {raw_table}")
    out = {}

    for row in res.result_rows:
        col_name = row[0]
        raw_type = row[1]
        is_nullable = raw_type.strip().startswith("Nullable(")
        base_type = _unwrap_ch_type(raw_type)

        out[col_name] = {
            "raw_type": raw_type,
            "base_type": base_type,
            "nullable": is_nullable,
        }

    return out


def _normalize_rows_by_ch_schema(rows, ch_col_meta: dict[str, dict]):
    fixed_rows = []

    for row in rows:
        fixed = dict(row)

        for col, meta in ch_col_meta.items():
            if col not in fixed:
                continue

            base_type = meta["base_type"]
            nullable = meta["nullable"]

            if base_type == "Date":
                val = _to_date(fixed[col])
                if val is None and not nullable:
                    val = FALLBACK_DATE
                fixed[col] = val

            elif base_type.startswith("DateTime"):
                val = _to_dt(fixed[col])
                val = _local_kz_to_utc(val)
                if val is None and not nullable:
                    val = FALLBACK_DATETIME
                fixed[col] = val

            else:
                fixed[col] = _decode_if_bytes(fixed[col])

        fixed_rows.append(fixed)

    return fixed_rows


def _debug_temporal_columns(rows, ch_col_meta: dict[str, dict]):
    temporal_cols = [
        col for col, meta in ch_col_meta.items()
        if meta["base_type"] == "Date" or meta["base_type"].startswith("DateTime")
    ]

    if not rows or not temporal_cols:
        return

    print("DEBUG temporal column sample types:")
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

    return wm_res.result_rows[0][0]


def _insert_watermark(ch_client, table_name: str, max_changed: datetime):
    ch_client.command(f"""
        INSERT INTO etl_watermarks (table_name, last_changed)
        VALUES ('{table_name}', toDateTime('{max_changed:%Y-%m-%d %H:%M:%S}'))
    """)


# =========================
# MAIN LOAD FUNCTION
# =========================
def incremental_load_table(table_name: str, raw_table: str):
    print(f"=== START incremental load: {table_name} -> {raw_table} ===")

    _, ch_client = _ch_client()

    last_changed = _get_watermark(ch_client, table_name)
    last_changed_with_overlap = last_changed - timedelta(minutes=OVERLAP_MINUTES)

    print(f"Watermark: {last_changed}")
    print(f"Overlap watermark: {last_changed_with_overlap}")

    ch_col_meta = _get_ch_column_meta(ch_client, raw_table)
    print(f"ClickHouse schema for {raw_table}: {ch_col_meta}")

    _, mysql = _mysql_conn()

    try:
        with mysql.cursor() as cursor:
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE changed >= %s
                ORDER BY changed, rid
            """
            cursor.execute(query, (last_changed_with_overlap,))
            rows = cursor.fetchall()

        print(f"Fetched {len(rows)} rows from MySQL for {table_name}")

        for r in rows[:DEBUG_SAMPLE_ROWS]:
            print(r)

        if not rows:
            print(f"No new rows for {table_name}")
            return

        rows = _normalize_rows_by_ch_schema(rows, ch_col_meta)
        _debug_temporal_columns(rows, ch_col_meta)

        columns = [col for col in ch_col_meta.keys() if col in rows[0]]
        data = [[row.get(col) for col in columns] for row in rows]

        ch_client.insert(
            table=raw_table,
            data=data,
            column_names=columns,
        )

        changed_values = [row.get("changed") for row in rows if row.get("changed") is not None]
        if not changed_values:
            raise ValueError(f"No valid changed values after normalization for table {table_name}")

        max_changed = max(changed_values)

        if max_changed > last_changed:
            _insert_watermark(ch_client, table_name, max_changed)
            print(
                f"Loaded {len(rows)} rows into {raw_table}. "
                f"Watermark updated to {max_changed:%Y-%m-%d %H:%M:%S}"
            )
        else:
            print(
                f"Loaded {len(rows)} overlap rows into {raw_table}. "
                f"Watermark not changed: {last_changed:%Y-%m-%d %H:%M:%S}"
            )

    finally:
        mysql.close()

    print(f"=== END incremental load: {table_name} ===")


# =========================
# DAG
# =========================
default_args = {
    "owner": "serzhan",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="mysql_clickhouse_increment",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["incremental", "mysql", "clickhouse", "raw", "stage"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # 1) basic dicts
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

    # 2) dict3 + dict4
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

    # 3) dict31 + dict32
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

    # 4) dict90 + dict91
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

    start >> [g_basic, g_34, g_3132, g_9091] >> end
