import json
import gzip
import csv
import datetime
import sys
from upstox_client import get_historical_candles, download_instrument_master

CATALOG = sys.argv[1] if len(sys.argv) > 1 else "stock_catalog"
SCHEMA = sys.argv[2] if len(sys.argv) > 2 else "dev"

VOL_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
DIR_INSTRUMENTS = f"{VOL_ROOT}/instruments"
DIR_DAILY = f"{VOL_ROOT}/candles_daily"
DIR_HOURLY = f"{VOL_ROOT}/candles_hourly"

WATCHLIST = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "SBIN"]

def resolve_instrument_keys(rows):
    keys = {}
    for row in rows:
        if row.get("segment") == "NSE_EQ" and row.get("trading_symbol") in WATCHLIST:
            keys[row["trading_symbol"]] = row["instrument_key"]
    return keys

def ingest_instrument_snapshot():
    """Full daily snapshot of NSE F&O + EQ rows for our watchlist underlyings — the AUTO CDC FROM SNAPSHOT subject."""
    tmp_path = "/tmp/instruments.csv.gz"
    download_instrument_master(tmp_path)
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    rows = []
    with gzip.open(tmp_path, "rt") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("name") or "").upper()
            if row.get("segment") in ("NSE_EQ", "NSE_FO") and any(w in name for w in WATCHLIST):
                rows.append(row)

    out_path = f"{DIR_INSTRUMENTS}/instruments_{ts}.json"
    with open(out_path, "w") as f:
        json.dump(rows, f)

    return resolve_instrument_keys(rows)

def ingest_candles(instrument_keys, unit, interval, lookback_days, dest_dir):
    today = datetime.date.today().isoformat()
    from_date = (datetime.date.today() - datetime.timedelta(days=lookback_days)).isoformat()
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    for symbol, instrument_key in instrument_keys.items():
        data = get_historical_candles(instrument_key, unit, interval, today, from_date)
        data["_symbol"] = symbol
        data["_ingested_at"] = datetime.datetime.utcnow().isoformat()
        with open(f"{dest_dir}/{symbol}_{ts}.json", "w") as f:
            json.dump(data, f)

if __name__ == "__main__":
    eq_keys = ingest_instrument_snapshot()
    ingest_candles(eq_keys, "days", "1", lookback_days=30, dest_dir=DIR_DAILY)
    ingest_candles(eq_keys, "hours", "1", lookback_days=7, dest_dir=DIR_HOURLY)
    print("Ingestion cycle complete.")