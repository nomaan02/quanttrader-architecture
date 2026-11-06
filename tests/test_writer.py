import fakeredis
import pytest

from services.writer import process_batch, run
from shared import streams

QUOTE = {"symbol": "DEMO.A", "timestamp": "2026-11-06T10:00:00+00:00", "lastPrice": 1.0, "bestBid": 0.9, "bestAsk": 1.1}


class ListSink:
    def __init__(self, fail_first: bool = False):
        self.rows: list[dict] = []
        self.fail_next = fail_first

    def write(self, quotes):
        if self.fail_next:
            self.fail_next = False
            raise ConnectionError("QuestDB unavailable")
        self.rows.extend(quotes)
        return len(quotes)


@pytest.fixture
def client():
    r = fakeredis.FakeRedis(decode_responses=True)
    streams.ensure_group(r, streams.STREAM_QUOTES, streams.GROUP_QUESTDB_WRITER)
    return r


def pending_count(client):
    return client.xpending(streams.STREAM_QUOTES, streams.GROUP_QUESTDB_WRITER)["pending"]


def test_ensure_group_is_idempotent(client):
    streams.ensure_group(client, streams.STREAM_QUOTES, streams.GROUP_QUESTDB_WRITER)


def test_writes_then_acknowledges(client):
    for i in range(3):
        streams.publish(client, streams.STREAM_QUOTES, QUOTE | {"lastPrice": float(i)})
    sink = ListSink()
    messages = streams.read_new(client, streams.STREAM_QUOTES, streams.GROUP_QUESTDB_WRITER, "w1")
    assert process_batch(client, sink, messages, streams.STREAM_QUOTES, streams.GROUP_QUESTDB_WRITER) == 3
    assert [q["lastPrice"] for q in sink.rows] == [0.0, 1.0, 2.0]
    assert pending_count(client) == 0


def test_failed_write_is_not_acknowledged_and_is_recovered_on_restart(client):
    streams.publish(client, streams.STREAM_QUOTES, QUOTE)
    failing = ListSink(fail_first=True)
    messages = streams.read_new(client, streams.STREAM_QUOTES, streams.GROUP_QUESTDB_WRITER, "w1")
    with pytest.raises(ConnectionError):
        process_batch(client, failing, messages, streams.STREAM_QUOTES, streams.GROUP_QUESTDB_WRITER)
    assert pending_count(client) == 1  # still owed

    # "Restart": run() recovers the pending message first, then stops.
    healthy = ListSink()
    assert run(client, healthy, "w1", stop=lambda: True) == 1
    assert healthy.rows == [QUOTE]
    assert pending_count(client) == 0


def test_malformed_messages_are_skipped_but_acknowledged(client):
    streams.publish(client, streams.STREAM_QUOTES, {"symbol": "DEMO.A"})
    sink = ListSink()
    messages = streams.read_new(client, streams.STREAM_QUOTES, streams.GROUP_QUESTDB_WRITER, "w1")
    assert process_batch(client, sink, messages, streams.STREAM_QUOTES, streams.GROUP_QUESTDB_WRITER) == 0
    assert sink.rows == []
    assert pending_count(client) == 0
