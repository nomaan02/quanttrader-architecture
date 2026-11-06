"""Print 1-minute OHLC bars built from the stored quotes.

    python scripts/query_bars.py            # all symbols, last 10 bars
    python scripts/query_bars.py DEMO.A 30  # one symbol, last 30 bars
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared.questdb_sink import exec_sql  # noqa: E402

symbol = sys.argv[1] if len(sys.argv) > 1 else None
limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
where = f"WHERE symbol = '{symbol.replace(chr(39), '')}'" if symbol else ""

sql = f"""
SELECT ts, symbol, first(price) open, max(price) high, min(price) low, last(price) close, sum(volume) volume
FROM quotes {where}
SAMPLE BY 1m ALIGN TO CALENDAR
ORDER BY ts DESC
LIMIT {limit};
"""
result = exec_sql(sql)
columns = [c["name"] for c in result["columns"]]
print("  ".join(f"{c:>20}" if c == "ts" else f"{c:>10}" for c in columns))
for row in result["dataset"]:
    print("  ".join(f"{str(v)[:19]:>20}" if i == 0 else f"{v:>10}" for i, v in enumerate(row)))
