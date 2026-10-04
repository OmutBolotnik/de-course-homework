# Завдання 4-6: Kafka consumer.
# Запуск із цієї директорії (homework/):  uv run python consumer.py
import json
import os
import time

from confluent_kafka import Consumer
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
    """Завдання 6 (20 балів).

    1. Створіть Consumer (bootstrap.servers=BOOTSTRAP_SERVERS, group.id=GROUP_ID,
       auto.offset.reset="earliest") і підпишіться на TOPIC.
    2. У циклі poll(1.0): пропускайте None та msg.error(); інакше
       json.loads(msg.value()) і update_counts(...). Рахуйте total.
    3. Зупиніться, коли топік мовчить IDLE_LIMIT_SECONDS поспіль. consumer.close().
    4. Зберіть stats = {"total", "by_type", "top_repos": top_repos(by_repo, 5)}
       і запишіть його JSON у OUTPUT_PATH (створіть каталог через os.makedirs).
       Поверніть stats.
    """
    raise NotImplementedError("Реалізуйте run_consumer")


if __name__ == "__main__":
    ic(run_consumer())
