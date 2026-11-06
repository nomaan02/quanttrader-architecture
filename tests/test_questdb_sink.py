from questdb.ingress import Buffer

from shared.questdb_sink import CREATE_QUOTES, to_row

QUOTE = {"symbol": "DEMO.A", "timestamp": "2026-11-06T10:00:00Z", "lastPrice": 5000.25, "bestBid": 5000.0, "bestAsk": 5000.5, "volume": 3}


def test_quote_renders_as_a_valid_ilp_line():
    buf = Buffer(protocol_version=1)  # v1 = plain-text floats, easy to read in a test
    buf.row("quotes", **to_row(QUOTE))
    line = bytes(buf).decode().strip()
    assert line.startswith("quotes,symbol=DEMO.A ")
    assert "price=5000.25" in line and "volume=3i" in line
    assert line.endswith(" 1793959200000000000")  # 2026-11-06T10:00:00Z in nanoseconds


def test_missing_volume_defaults_to_zero():
    row = to_row({k: v for k, v in QUOTE.items() if k != "volume"})
    assert row["columns"]["volume"] == 0


def test_table_deduplicates_on_timestamp_and_symbol():
    assert "DEDUP UPSERT KEYS(ts, symbol)" in CREATE_QUOTES
