import random
import time
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

PREFIX = "vs"

CONNECTION_ID = "goit_mysql_db"
SCHEMA = f"{PREFIX}_hw07"
TABLE = f"{SCHEMA}.medal_counts"
SOURCE_TABLE = "olympic_dataset.athlete_event_results"

MEDALS = ("Bronze", "Silver", "Gold")
MAX_RECORD_AGE_SECONDS = 30


def pick_medal():
    medal = random.choice(MEDALS)
    print(f"Picked medal: {medal}")
    return medal


def choose_count_task(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")
    return f"calc_{medal}"


def generate_delay(params):
    seconds = int(params["delay_seconds"])
    print(f"Sleeping for {seconds} seconds")
    time.sleep(seconds)


with DAG(
    dag_id=f"{PREFIX}_medal_counts",
    start_date=datetime(2024, 8, 4),
    schedule_interval=None,
    catchup=False,
    params={
        "delay_seconds": Param(
            5,
            type="integer",
            minimum=0,
            description=(
                f"Pause before the sensor; above {MAX_RECORD_AGE_SECONDS} "
                "makes the sensor fail."
            ),
        ),
    },
    tags=[PREFIX, "hw07"],
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CONNECTION_ID,
        sql=[
            f"CREATE DATABASE IF NOT EXISTS {SCHEMA};",
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10) NOT NULL,
                `count` INT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """,
        ],
    )

    pick_medal_task = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    branch_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=choose_count_task,
    )

    count_tasks = [
        SQLExecuteQueryOperator(
            task_id=f"calc_{medal}",
            conn_id=CONNECTION_ID,
            sql=f"""
            INSERT INTO {TABLE} (medal_type, `count`, created_at)
            SELECT '{medal}', COUNT(*), NOW()
            FROM {SOURCE_TABLE}
            WHERE medal = '{medal}';
            """,
        )
        for medal in MEDALS
    ]

    delay_task = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=CONNECTION_ID,
        sql=f"""
        SELECT IFNULL(
            TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= {MAX_RECORD_AGE_SECONDS},
            0
        )
        FROM {TABLE};
        """,
        mode="poke",
        poke_interval=5,
        timeout=15,
    )

    create_table >> pick_medal_task >> branch_task >> count_tasks
    count_tasks >> delay_task >> check_for_correctness
