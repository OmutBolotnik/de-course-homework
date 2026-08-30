"""github_archive_daily — ВАШ DAG. Специфікація: ../SPEC.md → «DAG».

Готові ETL-цеглинки вже є — імпортуйте і викликайте їх у задачах (не переписуйте):

    from include.gh_etl import download, validate, load_to_duckdb, summarize

Що треба зібрати (деталі й бали — у SPEC.md):
  * DAG `github_archive_daily`, розклад «щодня о 06:00 UTC», catchup=False;
  * усі задачі працюють із logical date {{ ds }}, а не datetime.now() — це дає
    ідемпотентність і коректний backfill;
  * граф:
        check_availability -> download_archive -> validate_file
            -> load_to_duckdb -> notify_completion
  * download_archive кладе шлях у XCom; validate_file і load_to_duckdb беруть його з XCom;
  * шляхи (дано):
        DB_PATH     = "/opt/airflow/data/github_analytics.duckdb"
        LANDING_DIR = "/opt/airflow/data/landing"

Перевірка: `airflow dags test github_archive_daily 2024-01-14` має пройти всі задачі;
наскрізно — `./verify.sh` із кореня homework/.
"""

from __future__ import annotations

import datetime
import logging

from airflow.decorators import dag, task

from gh_sensor import gh_archive_sensor
from include import gh_etl

log = logging.getLogger(__name__)

DB_PATH = "/opt/airflow/data/github_analytics.duckdb"
LANDING_DIR = "/opt/airflow/data/landing"


@dag(
    dag_id="github_archive_daily",
    schedule="0 6 * * *",
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["github", "archive", "daily"],
)
def github_archive_daily():
    check_availability = gh_archive_sensor.override(task_id="check_availability")(
        hour=14
    )

    @task
    def download_archive(ds: str) -> str:
        return gh_etl.download(ds, LANDING_DIR)

    @task
    def validate_file(path: str) -> str:
        gh_etl.validate(path)
        return path

    @task
    def load_to_duckdb(path: str, ds: str) -> int:
        return gh_etl.load_to_duckdb(path, ds, DB_PATH)

    @task
    def notify_completion(rows: int, ds: str) -> None:
        summary = gh_etl.summarize(ds, DB_PATH)
        log.info("[notify] ds=%s rows=%s summary=%s", ds, rows, summary)

    downloaded_path = download_archive()
    validated_path = validate_file(downloaded_path)
    loaded_rows = load_to_duckdb(validated_path)
    notify_completion(loaded_rows)

    check_availability >> downloaded_path


github_archive_daily()
