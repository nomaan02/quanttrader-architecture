"""Synthetic market-data producer.

Stands in for a live feed (the real platform uses the ProjectX Gateway market
hub and Databento). It emits quote events in the same awkward shape a real hub
sends (envelope + PascalCase keys, only changed fields), runs them through the
same normaliser and quote cache, and publishes complete quotes to a Redis Stream.

Prices are a random walk. Nothing here is real market data or a strategy.
"""
from __future__ import annotations

import logging
import os
import random
import signal
import time
from datetime import datetime, timezone

from shared import streams
from shared.projectx import QuoteCache, normalize_hub_payload

logging.basicConfig(level=logging.INFO, format="%(asctime)s producer %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SYMBOLS = {"DEMO.A": 5000.0, "DEMO.B": 18000.0}
TICKS_PER_SECOND = float(os.environ.get("TICKS_PER_SECOND", "5"))
_running = True


def fake_hub_event(symbol: str, price: float, rng: random.Random) -> dict:
    """A quote update shaped like a real hub message: envelope, PascalCase, partial fields."""
    spread = 0.25
    fields = {"Symbol": symbol, "LastPrice": round(price, 2), "Timestamp": datetime.now(timezone.utc).isoformat()}
    if rng.random() < 0.7:  # bid/ask don't change on every tick
        fields |= {"BestBid": round(price - spread, 2), "BestAsk": round(price + spread, 2)}
    if rng.random() < 0.5:
        fields["Volume"] = rng.randint(1, 20)
    return {"data": fields, "action": 1}


def main() -> None:
    def stop(*_):
        global _running
        _running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    client = streams.connect()
    cache = QuoteCache()
    rng = random.Random(int(os.environ.get("SEED", "42")))
    prices = dict(SYMBOLS)
    sent = 0
    log.info("publishing synthetic quotes for %s to %s", ", ".join(prices), streams.STREAM_QUOTES)

    while _running:
        for symbol in prices:
            prices[symbol] *= 1 + rng.gauss(0, 0.0002)
            quote = cache.update(symbol, normalize_hub_payload(fake_hub_event(symbol, prices[symbol], rng)))
            if {"lastPrice", "bestBid", "bestAsk"} <= quote.keys():
                streams.publish(client, streams.STREAM_QUOTES, quote)
                sent += 1
        if sent and sent % 500 < len(prices):
            log.info("%d quotes published", sent)
        time.sleep(1 / TICKS_PER_SECOND)
    log.info("stopped after %d quotes", sent)


if __name__ == "__main__":
    main()
