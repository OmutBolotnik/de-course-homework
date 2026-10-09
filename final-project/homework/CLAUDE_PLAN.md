# CLAUDE_PLAN.md

План виконання фінального проєкту. Джерела: `README.md`, `SPEC.md`, `tests/`, `scripts/check_dag.py`.
Зауваження з рев'ю коду — у `CLAUDE_NOTES.md`, випадки для журналу ШІ — у `CLAUDE_EXAMPLES.md`.
`NOTES.md` пишеш ти.

## Загальний план

| Етап | Що | Файли «ви» | Перевірка | Гілка (за README) |
|---|---|---|---|---|
| 1. Ingest | Kafka → landing, landing → Bronze | `stream/consumer.py`, `bronze_job.py` | `./verify.sh ingest` | `project-1-ingest` |
| 2. Transform | 6 dbt-моделей Silver/Gold + ≥ 2 власні тести | `dbt_rides/models/**`, `dbt_rides/tests/` | `./verify.sh transform` | `project-2-transform` |
| 3. Orchestrate | DAG Airflow + `NOTES.md` | `dags/rides_medallion.py`, `NOTES.md` | `./verify.sh orchestrate` | `project-3-orchestrate` |

Правила, що діють на всіх етапах:

- Код — **лише** у функціях/файлах, позначених «ви» / TODO. Решту не чіпати.
- Етап 2 не залежить від етапу 1 (є фікстури). Етап 3 потребує готових 1 і 2.
- Перед кожним PR: `./verify.sh lint` + `./verify.sh <етап>`; хвіст виводу — в опис PR,
  reviewer `@desireoftheother`.
- Перед `verify.sh ingest|transform` Airflow має бути зупинений (пише в ті самі таблиці).
- `uv` бере `homework/.venv`; активний venv кореня репо дає лише попередження (`deactivate`).
- Журнал ШІ для `NOTES.md` зручніше вести по ходу, а не наприкінці.
- **Інтеграція — на гілці `stage`** (від `main`, `--no-ff` merge усіх трьох гілок етапів; не
  пушиться). Фікс робиться в гілці свого етапу, комітиться й заново вливається в `stage`, щоб PR
  етапу містив виправлення. Повний `./verify.sh` — на `stage`.

---

## Етап 1 — Ingest

Статус: код готовий; `ruff`, `tests/test_consumer.py`, `tests/test_bronze.py` — зелені.

### 1а. Kafka → landing (`stream/consumer.py`)

- [x] **Крок 1. `landing_path`** — `ingested_at.astimezone(UTC)`;
      `base/dt=YYYY-MM-DD/hour=HH/part-p{p}-o{first:012d}-o{last:012d}.ndjson`.
- [x] **Крок 2. `write_batch`** — групування за партицією, сортування за офсетом;
      `.ndjson.tmp` у тій самій директорії → `fsync` → `os.replace` → `fsync` директорії;
      при збої `.tmp` видаляється, виняток прокидається як є; один об'єкт = один рядок
      (`b" ".join(value.splitlines()) + b"\n"`).
- [x] **Крок 3. `flush()`** — `write_batch(...)`, **потім** `consumer.commit(asynchronous=False)`.

### 1б. landing → Bronze (`bronze_job.py`)

- [x] **Крок 4. `EVENT_SCHEMA` + `read_landing`** — усі поля `StringType` (`payload` — сирий текст,
      перевірено байт-у-байт), `occurred_at` → `to_timestamp`; `recursiveFileLookup` (без `dt`/`hour`
      колонок) + `pathGlobFilter="*.ndjson"` (без `.tmp`); `_source_file` — сувора регулярка по
      `_metadata.file_path` + `raise_error` для файлів поза контрактом; `_ingested_at` — `current_timestamp()`.
- [x] **Крок 5. `select_new`** — порожня множина → `df`; інакше `~isin(...)`.
- [x] **Крок 6. `write_bronze`** — `coalesce(1)` → одна партиція → одна JDBC-транзакція.

###

- [x] `uv run mypy` (входить у `./verify.sh lint`).
- [x] `./verify.sh ingest` повністю — Kafka e2e, збій запису, `kill -9`.
- [x] Коміт, push, PR. 

---

## Етап 2 — Transform (dbt)

Статус: **готово.** `./verify.sh transform` зелений (6/6 у `test_incremental.py`, зокрема
incremental == full refresh) і на `project-2-transform`, і на `stage`; власні тести ламаються на
своїх поломках (FAIL 217 / FAIL 33); PR відкрито.

Цикл роботи — на фікстурах, батч за батчем, з «чужим» поясом сесії:

```bash
uv run python -m scripts.reset_warehouse
./dbt.sh seed
uv run python -m scripts.load_bronze_fixture 1
PGTZ=Pacific/Chatham ./dbt.sh build --selector silver --indirect-selection cautious
PGTZ=Pacific/Chatham ./dbt.sh build --selector gold   --indirect-selection cautious
PGTZ=Pacific/Chatham ./dbt.sh test  --selector reconcile
# звірка з SPEC 6.3, потім load_bronze_fixture 2, 3, 4 і знову build
```

Скрізь: `incremental_strategy='delete+insert'`, межа — `_ingested_at > {{ high_watermark() }}`
у `{% if is_incremental() %}`, дати — `{{ utc_date(...) }}`, `_loaded_at = '{{ run_started_at }}'::timestamptz`,
індекси — через `config(indexes=[…])` (створюються лише разом із таблицею: після зміни — `--full-refresh`).
Jinja-вирази (`{{ … }}`) — без SQL-коментарів `--` усередині.

- [x] **Крок 7. `gold/dim_zone`** (`table`) — `ref('seed_taxi_zone')`,
      `coalesce(nullif(trim(x), ''), 'Unknown')`, `UNION ALL` рядок `-1` / `'Unknown'`. Ціль: 266.

- [x] **Крок 8. `silver/events`** (`unique_key='event_id'`)
  - нові рядки Bronze за watermark; `source IS DISTINCT FROM 'loadtest'`, без NULL `occurred_at` / `ride_id`;
  - у межах запуску — `row_number() … ORDER BY _ingested_at, _source_file`;
  - дублікат із пізнішого запуску **пропускається** (`NOT EXISTS` до `{{ this }}`): інкремент =
    full refresh навіть в аудит-колонках, поїздка не перебудовується дарма;
  - індекси: `event_id` (unique), `ride_id`, `_ingested_at`.
  - Ціль: 1 367 / 2 717 / 4 071 / 5 447 після батчів 1–4.

- [x] **Крок 9. `silver/rides`** (`unique_key='ride_id'`)
  - `touched` = `ride_id` подій з `_ingested_at > high_watermark()`; згортка з **усієї** історії;
  - `DISTINCT ON (ride_id, event_type)` (найраніша за `occurred_at`) → CTE на тип →
    `FROM req LEFT JOIN …` (поїздка існує лише з `ride_requested`);
  - статус, фактичні зони, суми з `fare`, `tip` лишається NULL, `_ingested_at = max` усіх подій;
  - індекси: `ride_id` (unique), `_ingested_at`.
  - Ціль: таблиця 6.3 (rides / completed / cancelled / paid_at).

- [x] **Крок 10. `gold/dim_driver`** (`unique_key='driver_key'`)
  - `touched` = водії нових `ride_accepted`; атрибути — з усієї історії,
    `DISTINCT ON (driver_key) … ORDER BY occurred_at DESC, event_id DESC`;
  - член `'unknown'` — на першому запуску, в інкременті — `WHERE NOT EXISTS` (самовідновлюється,
    не перевставляється); `_ingested_at = NULL` (не піднімає watermark);
  - індекс: `driver_key` (unique).
  - Ціль: 81.

- [x] **Крок 11. `gold/fact_ride`** (`unique_key='ride_id'`)
  - рядки `ref('rides')` з `_ingested_at > high_watermark()`;
  - `LEFT JOIN` до `dim_zone` ×2 / `dim_driver` + `coalesce(-1 / 'unknown')`;
  - `requested_date = utc_date(…)`, `requested_hour = date_trunc('hour', … AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'`;
  - індекси: `ride_id` (unique), `requested_hour`, `_ingested_at`.
  - Ціль: 1 171.

- [x] **Крок 12. `gold/agg_zone_hourly`**
  - `touched_hours` = `requested_hour` нових рядків факту; ці години перераховуються **цілком**;
  - `unique_key='requested_hour'` — delete+insert прибирає й рядок старої зони;
  - унікальний індекс на grain `(requested_hour, pickup_zone_key)`.
  - Ціль: 351, `sum(gross_revenue) = 48 972.86`.

- [x] **Крок 13. Власні singular-тести**
  - `assert_tip_null_only_for_cash.sql` — крок `silver`;
  - `assert_driver_attributes_are_latest.sql` — тег `reconcile` (Silver + Gold), тай-брейк як у моделі.

- [x] **Крок 14. Фінальна звірка**
  - [x] `./verify.sh transform` — повторний прогін після фіксу парсингу;
  - [x] зламати кожен власний тест (`coalesce(tip, 0)` у `rides`; `latest` лише з батча в
        `dim_driver`), зберегти вивід для `NOTES.md`, повернути модель;
  - [x] коміт, push, PR `project-2-transform` з хвостом виводу.

---

## Етап 3 — Orchestrate (Airflow 2.10)

Статус: код готовий. На `stage` повний `./verify.sh` зелений (`PASS ✅ all`; етап 3 — 7/7 у
`test_orchestrate.py`). Перший прогін падав на `bronze_contract` (`CLAUDE_EXAMPLES.md`, випадок 1) —
виправлено. Лишилось: `NOTES.md` і здача.

- [x] **Крок 15. `dags/rides_medallion.py`**
  - `schedule="*/5 * * * *"` (рядком — так перевіряє `check_dag.py`), `catchup=False`,
    `max_active_runs=1`, `start_date=datetime(2024, 1, 1)`;
  - `default_args`: `retries=1`, `retry_delay` 1 хв, `execution_timeout` 20 хв;
  - п'ять `BashOperator`, `bronze_spark >> bronze_contract >> silver >> gold >> reconcile`:
    - `bronze_spark`: `cd {PROJECT} && python bronze_job.py`
    - `bronze_contract`: `{DBT} test --select source:bronze --indirect-selection cautious {DBT_DIRS}`
      — `cautious` обов'язковий: з `eager` сюди потрапляє `assert_bronze_silver_reconcile` і падає
      на першому запуску, поки `silver.events` нема;
    - `silver` / `gold`: `{DBT} build --selector … --indirect-selection cautious {DBT_DIRS}`
    - `reconcile`: `{DBT} test --selector reconcile {DBT_DIRS}`
  - без `data_interval_start/end`, без `--select tag:`, без `env=` в операторах (успадковують
    оточення воркера — тест підміняє `LANDING_DIR` саме так).

- [x] **Крок 16. Перевірка на `stage`**
  - [x] precheck: заглушок нема (`scripts.precheck orchestrate` → `exit 0`);
  - [x] `./verify.sh ingest`, `./verify.sh transform` — зелені;
  - [x] ручний перший запуск (`reset_warehouse`, батчі 1–2, `airflow dags test rides_medallion
        2024-01-15`): 5/5 SUCCESS, 2 874 / 2 717 / 695 = SPEC 6.3;
  - [x] повний `./verify.sh` на `stage`: lint, ingest (14 + 3 + 8), transform (1 + 6),
        orchestrate (7) — `PASS ✅ all: усе зелене.`;
  - [ ] за бажання: `check_dag.py` у контейнері, `./up.sh --airflow --run`.

- [ ] **Крок 17. `NOTES.md`** (пишеш ти)
  - розділ 1 — рішення й компроміси (8 питань із шаблону); матеріал — `CLAUDE_NOTES.md`;
  - розділ 2 — журнал ШІ, мінімум 3 випадки з доказами; матеріал — `CLAUDE_EXAMPLES.md`
    (найсильніші — випадки 1–3);
  - розділ 3 — власні dbt-тести і як перевірено, що вони ловлять поломку (випадок 8; логи
    ламання — `CLAUDE_LOG_break_*.log`; логи DAG-у до/після фіксу — `CLAUDE_LOG_dag_first_run_*.log`);
  - розділ 4 — що далі.

- [ ] **Крок 18. Здача**
  - [x] на `stage`: `./verify.sh` повністю;
  - після `NOTES.md` — ще раз `./verify.sh lint` (NOTES.md не впливає, але це остання перевірка);
  - `NOTES.md` — коміт у `project-3-orchestrate`, влити в `stage`;
  - push `project-3-orchestrate`, PR з хвостом виводу `verify.sh orchestrate`;
  - вирішити, чи комітити `CLAUDE_*.md` (або в `.gitignore`).
