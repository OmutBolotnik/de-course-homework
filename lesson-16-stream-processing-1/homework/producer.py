# Завдання 3: Kafka producer.
# Запуск із цієї директорії (homework/):  uv run python producer.py
import gzip
import json
import urllib.request
from typing import Iterator

from confluent_kafka import Producer
from icecream import ic

from transform import event_filter, flatten_event

# Дано, не редагувати.
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "github-events"
ARCHIVE_URL = "https://data.gharchive.org/2024-01-15-14.json.gz"
MAX_RAW = 100_000

# gharchive returns HTTP 403 to urllib's default User-Agent, so set our own.
_USER_AGENT = "de-course-homework/1.0"


def iter_archive(url: str, max_raw: int) -> Iterator[dict]:
    """Дано, не редагувати.

    Yield up to `max_raw` raw GitHub Archive events (parsed JSON dicts).
    Records arrive in the file's original (roughly chronological) order. No
    filtering or flattening happens here — that is your job in transform.py.
    """
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        with gzip.GzipFile(fileobj=response) as gz:
            for count, line in enumerate(gz):
                if count >= max_raw:
                    break
                yield json.loads(line)


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def run_producer() -> int:
    producer = build_producer()
    delivery_errors = []

    def on_delivery(err, _msg):
        if err is not None:
            delivery_errors.append(err)

    sent = 0
    for event in iter_archive(ARCHIVE_URL, MAX_RAW):
        if not event_filter(event):
            continue
        record = flatten_event(event)
        key = record["repo_name"].encode("utf-8")
        value = json.dumps(record).encode("utf-8")
        while True:
            try:
                producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(1)
        producer.poll(0)
        sent += 1

    undelivered = producer.flush(30)
    if undelivered or delivery_errors:
        raise RuntimeError(
            f"delivery failed: {len(delivery_errors)} errors, "
            f"{undelivered} still queued, {sent} produced"
        )
    return sent


if __name__ == "__main__":
    ic(run_producer())
