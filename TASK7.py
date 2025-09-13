# dags/olympic_medals_branch_dag.py
from __future__ import annotations

import random
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

# Load
MYSQL_CONN_ID = "mysql_olympic"
DB_NAME = "olympic_dataset"
TARGET_TABLE = f"{DB_NAME}.medal_counts"
SOURCE_TABLE = f"{DB_NAME}.athlete_event_results"

# Change on 35
DELAY_SECONDS = 5

default_args = {
    "owner": "student",
    "retries": 0,
}

with DAG(
    dag_id="olympic_medals_branch_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["homework", "airflow", "mysql", "branching"],
) as dag:

    # Create table (IF NOT EXISTS)
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(16) NOT NULL,
            count INT NOT NULL,
            created_at DATETIME NOT NULL
        ) ENGINE=InnoDB;
        """,
    )

    # Random choice
    def pick_medal(**_):
        return random.choice(["bronze_branch", "silver_branch", "gold_branch"])

    branch = BranchPythonOperator(
        task_id="pick_random_medal",
        python_callable=pick_medal,
    )

    # Table record
    bronze_task = MySqlOperator(
        task_id="bronze_branch",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {TARGET_TABLE} (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM {SOURCE_TABLE}
        WHERE medal = 'Bronze';
        """,
    )

    silver_task = MySqlOperator(
        task_id="silver_branch",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {TARGET_TABLE} (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM {SOURCE_TABLE}
        WHERE medal = 'Silver';
        """,
    )

    gold_task = MySqlOperator(
        task_id="gold_branch",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        INSERT INTO {TARGET_TABLE} (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM {SOURCE_TABLE}
        WHERE medal = 'Gold';
        """,
    )

    # Merge branch
    join_after_branch = EmptyOperator(
        task_id="join_after_branch",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Delay of the next task
    def sleep_n_seconds(n: int):
        time.sleep(n)

    delay = PythonOperator(
        task_id="delay_task",
        python_callable=sleep_n_seconds,
        op_kwargs={"n": DELAY_SECONDS},
    )

    # Sensor
    def is_latest_record_fresh(**_):
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        # Return a few seconds
        sql = f"SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) AS diff_sec FROM {TARGET_TABLE};"
        row = hook.get_first(sql)
        if not row or row[0] is None:
            return False  # still no records
        diff_sec = int(row[0])
        return diff_sec <= 30

    freshness_sensor = PythonSensor(
        task_id="freshness_sensor_le_30s",
        python_callable=is_latest_record_fresh,
        poke_interval=5,   # how often to check
        timeout=60,        # total timeout of sensor
        mode="poke",       # "reschedule" 
        soft_fail=False,   # if True — no add DAG
    )

    # Dependencies
    create_table >> branch
    branch >> [bronze_task, silver_task, gold_task] >> join_after_branch
    join_after_branch >> delay >> freshness_sensor
