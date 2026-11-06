"""Redis Streams helpers: durable, at-least-once delivery between services.

Why Streams rather than plain pub/sub: pub/sub drops a message if nobody is
listening at that instant. A Stream keeps it, and a *consumer group* tracks
what each consumer has received and acknowledged, so a service that crashes
mid-message picks it up again on restart (the "pending entries list").
"""
from __future__ import annotations

import json
import logging
import os

import redis

logger = logging.getLogger(__name__)

STREAM_QUOTES = "stream:quotes"
GROUP_QUESTDB_WRITER = "questdb_writer"

# Keep roughly the last N entries per stream so memory can't grow forever.
STREAM_MAXLEN = int(os.environ.get("STREAM_MAXLEN", "10000"))


def connect(url: str | None = None) -> redis.Redis:
    """One shared connection pool per process. Reads REDIS_URL by default."""
    return redis.Redis.from_url(
        url or os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5,
        health_check_interval=30,
    )


def publish(client: redis.Redis, stream: str, data: dict) -> str:
    """Append one message. Returns its stream ID (e.g. '1730912345678-0')."""
    return client.xadd(stream, {"payload": json.dumps(data, separators=(",", ":"))}, maxlen=STREAM_MAXLEN, approximate=True)


def ensure_group(client: redis.Redis, stream: str, group: str) -> None:
    """Create a consumer group if it doesn't exist yet. Safe to call on every start."""
    try:
        client.xgroup_create(stream, group, id="0", mkstream=True)
        logger.info("created consumer group %s on %s", group, stream)
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def _decode(entries) -> list[tuple[str, dict]]:
    messages = []
    for msg_id, fields in entries:
        if not fields:  # already-acknowledged placeholder when reading history
            continue
        try:
            messages.append((msg_id, json.loads(fields.get("payload", "{}"))))
        except ValueError:
            logger.warning("skipping unreadable message %s", msg_id)
            messages.append((msg_id, {}))
    return messages


def read_new(client: redis.Redis, stream: str, group: str, consumer: str, count: int = 500, block_ms: int = 1000) -> list[tuple[str, dict]]:
    """Messages never delivered to anyone in this group. Waits up to block_ms."""
    result = client.xreadgroup(group, consumer, {stream: ">"}, count=count, block=block_ms)
    return _decode(result[0][1]) if result else []


def read_pending(client: redis.Redis, stream: str, group: str, consumer: str, count: int = 500) -> list[tuple[str, dict]]:
    """Messages this consumer received but never acknowledged, e.g. because it crashed. Call on start-up."""
    result = client.xreadgroup(group, consumer, {stream: "0"}, count=count)
    return _decode(result[0][1]) if result else []


def ack(client: redis.Redis, stream: str, group: str, msg_ids: list[str]) -> int:
    """Mark messages as done so they are never redelivered."""
    return client.xack(stream, group, *msg_ids) if msg_ids else 0
