# Завдання 4-6: Kafka consumer.
# Запуск із цієї директорії (homework/):  uv run python consumer.py
import json
import os
import time

from confluent_kafka import Consumer, KafkaException
from icecream import ic

# Дано, не редагувати.
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "github-events"
OUTPUT_PATH = "data/output/stats.json"

GROUP_ID = "github-stats-consumer"
IDLE_LIMIT_SECONDS = 5.0  # зупинитись, коли топік мовчить стільки секунд


def update_counts(by_type: dict, by_repo: dict, event: dict) -> None:
    event_type = event["event_type"]
    repo_name = event["repo_name"]
    by_type[event_type] = by_type.get(event_type, 0) + 1
    by_repo[repo_name] = by_repo.get(repo_name, 0) + 1


def top_repos(by_repo: dict, n: int = 5) -> list:
    ranked = sorted(by_repo.items(), key=lambda kv: (-kv[1], kv[0]))
    return [[name, count] for name, count in ranked[:n]]


def run_consumer() -> dict:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    last_activity = None

    def on_assign(_consumer, _partitions):
        nonlocal last_activity
        last_activity = time.monotonic()

    consumer.subscribe([TOPIC], on_assign=on_assign)

    by_type: dict = {}
    by_repo: dict = {}
    total = 0
    try:
        while last_activity is None or time.monotonic() - last_activity < IDLE_LIMIT_SECONDS:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            err = msg.error()
            if err:
                if err.fatal():
                    raise KafkaException(err)
                ic(err)
                continue
            last_activity = time.monotonic()
            try:
                update_counts(by_type, by_repo, json.loads(msg.value()))
            except (ValueError, KeyError, TypeError) as exc:
                ic(msg.partition(), msg.offset(), exc)
                continue
            total += 1

        stats = {
            "total": total,
            "by_type": by_type,
            "top_repos": top_repos(by_repo, 5),
        }
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        tmp_path = OUTPUT_PATH + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(stats, f, indent=2)
        os.replace(tmp_path, OUTPUT_PATH)
        if total:
            consumer.commit(asynchronous=False)
        return stats
    finally:
        consumer.close()


if __name__ == "__main__":
    ic(run_consumer())
