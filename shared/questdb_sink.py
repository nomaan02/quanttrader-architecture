"""Write quotes into QuestDB, a time-series database.

- Writes use InfluxDB Line Protocol (ILP) over HTTP: fast, batched, and the
  server confirms each flush, so we only acknowledge Redis messages after a
  successful write.
- The table is created with DEDUP UPSERT KEYS(ts, symbol). If a message is
  redelivered after a crash, the duplicate row is merged rather than doubled.
- Queries use QuestDB's REST endpoint (/exec), which speaks SQL.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.parse
import urllib.request
from datetime import datetime

logger = logging.getLogger(__name__)

ILP_CONF = os.environ.get("QDB_ILP_CONF", "http::addr=localhost:9000;")
REST_URL = os.environ.get("QDB_REST_URL", "http://localhost:9000")

CREATE_QUOTES = """
CREATE TABLE IF NOT EXISTS quotes (
    ts TIMESTAMP,
    symbol SYMBOL,
    price DOUBLE,
    bid DOUBLE,
    ask DOUBLE,
    volume LONG
) TIMESTAMP(ts) PARTITION BY DAY WAL
DEDUP UPSERT KEYS(ts, symbol);
"""


def exec_sql(sql: str, rest_url: str = REST_URL, timeout: int = 30) -> dict:
    """Run SQL through QuestDB's REST API and return the JSON response."""
    url = f"{rest_url.rstrip('/')}/exec?{urllib.parse.urlencode({'query': sql})}"
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read())


def ensure_schema(rest_url: str = REST_URL) -> None:
    exec_sql(CREATE_QUOTES, rest_url)


def to_row(quote: dict) -> dict:
    """One normalised quote -> the arguments for an ILP row on the `quotes` table."""
    return {
        "symbols": {"symbol": quote["symbol"]},
        "columns": {
            "price": float(quote["lastPrice"]),
            "bid": float(quote["bestBid"]),
            "ask": float(quote["bestAsk"]),
            "volume": int(quote.get("volume", 0)),
        },
        "at": datetime.fromisoformat(quote["timestamp"].replace("Z", "+00:00")),
    }


class QuoteSink:
    """Batches quotes and flushes them to QuestDB in one HTTP request."""

    def __init__(self, conf: str = ILP_CONF) -> None:
        from questdb.ingress import Sender  # imported here so the rest of the module works without the client

        self._sender = Sender.from_conf(conf)
        self._sender.establish()

    def write(self, quotes: list[dict]) -> int:
        for quote in quotes:
            self._sender.row("quotes", **to_row(quote))
        self._sender.flush()  # raises if QuestDB rejects the batch
        return len(quotes)

    def close(self) -> None:
        self._sender.close()
