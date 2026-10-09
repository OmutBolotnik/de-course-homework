"""DAG: Bronze (Spark) -> Silver -> Gold -> reconcile. ЕТАП 3 — створіть DAG за SPEC.md, розділ 5.

Контейнер Airflow має Java, pyspark, JDBC-драйвер і dbt (в окремому venv, див.
docker/Dockerfile.airflow). Проєкт змонтовано в /opt/airflow/project.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/project"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"  # dbt у окремому venv
# target/ і logs/ пишемо в /tmp контейнера, щоб не смітити у змонтованому проєкті.
DBT = f"cd {PROJECT} && DBT_TARGET_PATH=/tmp/dbt-target DBT_LOG_PATH=/tmp/dbt-logs {DBT_BIN}"
DBT_DIRS = "--project-dir dbt_rides --profiles-dir dbt_rides"

default_args = {
    "owner": "data-platform",
    # Ретрай безпечний: Bronze ідемпотентний за файлом і атомарний, dbt-моделі — за watermark.
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=20),
}

# Стан «що вже оброблено» живе в даних (_source_file у Bronze, high_watermark() у моделях), а не в
# розкладі: жодна задача не бере data_interval_start/end, тож пропущений чи повторний запуск
# просто підхоплює все, що з'явилося від останнього успішного.
with DAG(
    dag_id="rides_medallion",
    description="Bronze (Spark) -> Silver -> Gold -> reconcile",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["final-project"],
) as dag:
    # env не задаємо: BashOperator успадковує оточення воркера (PGHOST, LANDING_DIR, PG_JDBC_JAR…).
    bronze_spark = BashOperator(
        task_id="bronze_spark",
        bash_command=f"cd {PROJECT} && python bronze_job.py",
    )

    # cautious — у всіх dbt-кроках: тест, у якого parent з іншого шару, у цьому кроці не біжить
    # (він — у reconcile), замість того щоб падати на ще не перебудованому шарі. Тут це критично:
    # з типовим eager `--select source:bronze` підхопив би assert_bronze_silver_reconcile, а на
    # першому запуску silver.events ще нема -> Database Error.
    bronze_contract = BashOperator(
        task_id="bronze_contract",
        bash_command=(
            f"{DBT} test --select source:bronze --indirect-selection cautious {DBT_DIRS}"
        ),
    )

    silver = BashOperator(
        task_id="silver",
        bash_command=f"{DBT} build --selector silver --indirect-selection cautious {DBT_DIRS}",
    )

    gold = BashOperator(
        task_id="gold",
        bash_command=f"{DBT} build --selector gold --indirect-selection cautious {DBT_DIRS}",
    )

    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=f"{DBT} test --selector reconcile {DBT_DIRS}",
    )

    bronze_spark >> bronze_contract >> silver >> gold >> reconcile
