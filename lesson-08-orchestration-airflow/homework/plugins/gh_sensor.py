"""GHArchiveSensor — ВАШ custom sensor. Специфікація: ../../SPEC.md → «Sensor».

Сенсор чекає, поки годинний файл GitHub Archive за logical date стане доступним,
і лише тоді пропускає DAG далі.

Підказки:
  * успадкуйте `airflow.sensors.base.BaseSensorOperator`;
  * у __init__ прийміть параметр `hour` (година доби, яку перевіряємо);
  * реалізуйте `poke(self, context) -> bool`: візьміть дату з context["ds"],
    зберіть URL https://data.gharchive.org/<ds>-<hour>.json.gz і зробіть HTTP HEAD —
    поверніть True на 200, інакше False (або при винятку);
  * у DAG додайте сенсор першою задачею з timeout=600, poke_interval=60,
    mode="reschedule".
"""

from __future__ import annotations

import logging
import urllib.error
import urllib.request

from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue

log = logging.getLogger(__name__)


@task.sensor(poke_interval=60, timeout=600, mode="reschedule")
def gh_archive_sensor(hour: int, **context) -> PokeReturnValue:
    ds = context["ds"]
    url = f"https://data.gharchive.org/{ds}-{hour:02d}.json.gz"
    req = urllib.request.Request(
        url, method="HEAD", headers={"User-Agent": "gh-etl/1.0"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            log.info("HEAD %s -> %s", url, resp.status)
            return PokeReturnValue(is_done=resp.status == 200)
    except urllib.error.HTTPError as exc:
        log.info("HEAD %s -> %s", url, exc.code)
        return PokeReturnValue(is_done=False)
    except urllib.error.URLError as exc:
        log.warning("HEAD %s failed: %s", url, exc)
        return PokeReturnValue(is_done=False)
