from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

MYSQL_CONN_ID = "tourservice_mysql"
CH_CONN_ID = "clickhouse"

def _bash_cmd():
    mysql = BaseHook.get_connection(MYSQL_CONN_ID)
    ch = BaseHook.get_connection(CH_CONN_ID)

    mysql_host = mysql.host
    mysql_port = int(mysql.port or 3306)
    mysql_user = mysql.login
    mysql_pass = mysql.password
    mysql_db   = mysql.schema  # www

    ch_host = ch.host
    ch_http_port = int(ch.port or 8123)  # у тебя 8123
    ch_user = ch.login or "default"
    ch_pass = ch.password or ""
    ch_db   = ch.schema or "fondkamkor"

    # Твой SQL (без изменений)
    src_sql = f"""
    select
    		d.rid				as rid,
    	    d.number			as number,
    	    d.country1_id		as country1_id, 
    	    d.country2_id		as country2_id, 
    	    d.country3_id		as country3_id, 
    	    d.country4_id		as country4_id, 
    	    d.country5_id		as country5_id, 
    	    d.country6_id		as country6_id,
    	    d.currency			as currency,
    	    d.date_start		as date_start, 
    	    d.date_end			as date_end,
    	    d.touragent_bin		as touragent_bin,
    	    d.airport_start		as airport_start, 
    	    d.airport_end		as airport_end,
    	    d.flight_start		as flight_start, 
    	    d.flight_end		as flight_end,
    	    d.airlines			as airlines,
    	    d.from_cabinet		as from_cabinet,
    	    d.passport			as passport,
    	    d.tid				as tid,
    	    d.qid				as qid,
    	    d.created			as created,
    	    d.changed			as changed,
    	    d.user				as user,
    	    d.enabled			as enabled,
            coalesce(d2.rid, 0) as sub_rid,
    	    d2.bindrid    		as sub_bindrid,
    	    d2.sub_date_start	as sub_date_start,
    	    d2.sub_date_end		as sub_date_end,
    	    d2.sub_airport		as sub_airport,
    	    d2.sub_airlines		as sub_airlines,
    	    d2.sub_flight		as sub_flight,
    	    d2.changed    		as sub_changed,
    	    d2.user       		as sub_user,
    	    d2.enabled    		as sub_enabled
    from `{mysql_db}`.`dict90` d
    left join `{mysql_db}`.`dict91` d2 on d.rid = d2.bindrid
    """

    # ВАЖНО: --batch --raw --silent + табы в TSV
    # ВАЖНО: на ClickHouse стороне указываем FORMAT TSV и input_format_null_as_default=1
    # чтобы Nullable/UInt нормально вставлялись (0 уже в SQL для sub_rid)
    cmd = f"""
set -euo pipefail

clickhouse-client --host {ch_host} --port {ch_http_port} --protocol http \
  --user '{ch_user}' --password '{ch_pass}' \
  --database '{ch_db}' \
  --query "TRUNCATE TABLE `{ch_db}`.`dict90_flat`"

mysql --host={mysql_host} --port={mysql_port} --user='{mysql_user}' --password='{mysql_pass}' \
  --database='{mysql_db}' \
  --batch --raw --silent \
  -e "{src_sql.replace('"', '\\"').strip()}" \
| clickhouse-client --host {ch_host} --port {ch_http_port} --protocol http \
  --user '{ch_user}' --password '{ch_pass}' \
  --database '{ch_db}' \
  --query "INSERT INTO `{ch_db}`.`dict90_flat` SETTINGS input_format_null_as_default=1 FORMAT TSV"
"""
    return cmd

with DAG(
    dag_id="sync_mysql_to_clickhouse_dict90_flat_pipe",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sync", "mysql", "clickhouse", "flat", "pipe"]
) as dag:

    load_flat_pipe = BashOperator(
        task_id="load_dict90_flat_pipe",
        bash_command=_bash_cmd(),
    )
