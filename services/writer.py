"""Stream → QuestDB writer.

Reads quotes from a Redis Stream as part of a consumer group and writes them
to QuestDB in batches. A batch is acknowledged in Redis only AFTER QuestDB has
confirmed the write, so a crash never loses data: on restart, the writer first
re-processes anything it had received but not acknowledged.
"""
from __future__ import annotations

import logging
import os
import signal
import socket
import time
from typing import Protocol

from shared import streams

logging.basicConfig(level=logging.INFO, format="%(asctime)s writer %(levelname)s %(message)s")
log = logging.getLogger(__name__)

REQUIRED = ("symbol", "timestamp", "lastPrice", "bestBid", "bestAsk")


class Sink(Protocol):
    def write(self, quotes: list[dict]) -> int: ...


def process_batch(client, sink: Sink, messages: list[tuple[str, dict]], consumer_stream: str, group: str) -> int:
    """Write a batch, then acknowledge it. Malformed messages are acknowledged and skipped (logged)."""
    good = [data for _, data in messages if all(k in data for k in REQUIRED)]
    skipped = len(messages) - len(good)
    if skipped:
        log.warning("skipping %d malformed message(s)", skipped)
    if good:
        sink.write(good)  # raises on failure, so nothing below runs and nothing is acknowledged
    streams.ack(client, consumer_stream, group, [msg_id for msg_id, _ in messages])
    return len(good)


def run(client, sink: Sink, consumer: str, stop=lambda: False, batch: int = 500) -> int:
    streams.ensure_group(client, streams.STREAM_QUOTES, streams.GROUP_QUESTDB_WRITER)

    pending = streams.read_pending(client, streams.STREAM_QUOTES, streams.GROUP_QUESTDB_WRITER, consumer, count=batch)
    total = 0
    while pending:  # crash recovery: finish what we had received but not acknowledged
        log.info("recovering %d unacknowledged message(s)", len(pending))
        total += process_batch(client, sink, pending, streams.STREAM_QUOTES, streams.GROUP_QUESTDB_WRITER)
        pending = streams.read_pending(client, streams.STREAM_QUOTES, streams.GROUP_QUESTDB_WRITER, consumer, count=batch)

    while not stop():
        messages = streams.read_new(client, streams.STREAM_QUOTES, streams.GROUP_QUESTDB_WRITER, consumer, count=batch)
        if messages:
            total += process_batch(client, sink, messages, streams.STREAM_QUOTES, streams.GROUP_QUESTDB_WRITER)
    return total


def main() -> None:
    from shared.questdb_sink import QuoteSink, ensure_schema

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    for attempt in range(30):  # QuestDB can take a few seconds to start
        try:
            ensure_schema()
            break
        except OSError as exc:
            log.info("waiting for QuestDB (%s)", exc)
            time.sleep(2)
    else:
        raise SystemExit("QuestDB did not become ready")

    client = streams.connect()
    sink = QuoteSink()
    consumer = os.environ.get("CONSUMER_NAME", socket.gethostname())
    log.info("consumer %s reading %s", consumer, streams.STREAM_QUOTES)
    try:
        total = run(client, sink, consumer, stop=lambda: stopping)
    finally:
        sink.close()
    log.info("stopped after writing %d quotes", total)


if __name__ == "__main__":
    main()
