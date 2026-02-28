# dag_dashboards_refresh.py
from __future__ import annotations

import time
from datetime import datetime
from typing import Dict

import clickhouse_connect

from airflow import DAG
from airflow.datasets import Dataset
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


# =========================
# CONFIG
# =========================
CH_CONN_ID = "clickhouse"
CH_DB = None  # None -> conn.schema (у тебя 'fondkamkor')

# Dataset, который публикует базовый DAG после успешного обновления dict* / flat*
BASE_TABLES_READY = Dataset("ch://fondkamkor/base_tables_ready")


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
# DASHBOARD QUERIES
# =========================
def _dashboard_inserts(ch_db: str) -> Dict[str, str]:
    """
    Map: dashboard_table_name -> INSERT SELECT sql
    IMPORTANT: SQL includes target table name.
    """
    d5 = f"`{ch_db}`.`dashboard_5`"
    d12 = f"`{ch_db}`.`dashboard_12`"
    d19 = f"`{ch_db}`.`dashboard_19`"

    dict3_flat = f"`{ch_db}`.`dict3_flat`"
    dict31_flat = f"`{ch_db}`.`dict31_flat`"
    dict90_flat = f"`{ch_db}`.`dict90_flat`"
    dict13 = f"`{ch_db}`.`dict13`"

    sql_12 = f"""
    INSERT INTO {d12}
    SELECT
        t3.created                  AS created,
        t.d3_orgname                AS orgname,
        t.d4_allow_auto_tour        AS allow_auto_tour,
        t3.from_cabinet             AS from_cabinet,
        t.d4_list                   AS list,
        t3.passport                 AS passport,
        t2.d32_qid                  AS qid
    FROM {dict3_flat} t
    JOIN {dict31_flat} t2
        ON t.d4_rid = t2.d31_operatorid
    LEFT JOIN {dict90_flat} t3
        ON t3.tid = t.d4_rid AND t3.qid = t2.d32_qid
    LEFT JOIN {dict13} t4
        ON t3.country1_id = t4.rid
    WHERE
          t2.d32_enabled = 1
      AND t2.d32_mode = 0
      AND t2.d32_qid > 0
    """

    sql_19 = f"""
    INSERT INTO {d19}
    SELECT
        t3.created                              AS created,
        toYear(t3.created)                      AS year,
        toMonth(t3.created)                     AS month,
        CASE
            WHEN toMonth(t3.created)=1  THEN 'Январь'
            WHEN toMonth(t3.created)=2  THEN 'Февраль'
            WHEN toMonth(t3.created)=3  THEN 'Март'
            WHEN toMonth(t3.created)=4  THEN 'Апрель'
            WHEN toMonth(t3.created)=5  THEN 'Май'
            WHEN toMonth(t3.created)=6  THEN 'Июнь'
            WHEN toMonth(t3.created)=7  THEN 'Июль'
            WHEN toMonth(t3.created)=8  THEN 'Август'
            WHEN toMonth(t3.created)=9  THEN 'Сентябрь'
            WHEN toMonth(t3.created)=10 THEN 'Октябрь'
            WHEN toMonth(t3.created)=11 THEN 'Ноябрь'
            WHEN toMonth(t3.created)=12 THEN 'Декабрь'
            ELSE NULL
        END                                     AS month_russian,
        t.d3_orgname                            AS orgname,
        t2.d32_qid                              AS qid,
        t4.country                              AS country
    FROM {dict3_flat} t
    JOIN {dict31_flat} t2
        ON t.d4_rid = t2.d31_operatorid
    LEFT JOIN {dict90_flat} t3
        ON t3.tid = t.d4_rid AND t3.qid = t2.d32_qid
    LEFT JOIN {dict13} t4
        ON t3.country1_id = t4.rid
    WHERE
          t2.d32_enabled = 1
      AND t2.d32_mode = 0
      AND t2.d32_qid > 0
      AND toYear(t3.created) != 1970
    """

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
        t.d3_orgname                            AS touragent,
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
    LEFT JOIN {dict13} t4
        ON t3.country1_id = t4.rid
    WHERE
          t2.d32_enabled = 1
      AND t2.d32_mode = 0
      AND t2.d32_qid > 0
    """

    return {
        "dashboard_5": sql_5,
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


# =========================
# DAG DEFINITION
# =========================
default_args = {
    "owner": "serzhan",
    "retries": 2,
    "retry_delay": 60,  # seconds
}

with DAG(
    dag_id="dashboards_refresh_serzhan",
    start_date=datetime(2024, 1, 1),
    schedule=[BASE_TABLES_READY],  # <-- запускается после базового DAG через Dataset
    catchup=False,
    default_args=default_args,
    tags=["clickhouse", "dashboards", "full_refresh"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Можно параллельно, но чтобы не убить CH, делаем цепочку (как ты любишь — контролировать нагрузку)
    with TaskGroup(group_id="refresh_dashboards") as g:
        d5 = PythonOperator(task_id="refresh_dashboard_5", python_callable=lambda: refresh_one_dashboard("dashboard_5"))
        d12 = PythonOperator(task_id="refresh_dashboard_12", python_callable=lambda: refresh_one_dashboard("dashboard_12"))
        d19 = PythonOperator(task_id="refresh_dashboard_19", python_callable=lambda: refresh_one_dashboard("dashboard_19"))

        # порядок можно менять
        d5 >> d12 >> d19

    start >> g >> end
