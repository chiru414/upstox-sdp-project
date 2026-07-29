import json
import gzip
import csv
import io
import datetime
from upstox_client import (
    get_profile, get_holdings, get_order_book,
    get_historical_candles, download_instrument_master,
)
import sys

CATALOG = sys.argv[1] if len(sys.argv) > 1 else "stock_catalog"
SCHEMA = sys.argv[2] if len(sys.argv) > 2 else "dev"

VOL_TICKS = f"/Volumes/{CATALOG}/{SCHEMA}/raw_ticks"
VOL_HOLDINGS = f"/Volumes/{CATALOG}/{SCHEMA}/raw_holdings"
VOL_ORDERS = f"/Volumes/{CATALOG}/{SCHEMA}/raw_orders"

WATCHLIST = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "SBIN"]

def resolve_instrument_keys():
    """Downloads the instrument master and resolves instrument_key for our watchlist."""
    tmp_path = "/tmp/instruments.csv.gz"
    download_instrument_master(tmp_path)
    keys = {}
    with gzip.open(tmp_path, "rt") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("segment") == "NSE_EQ" and row.get("trading_symbol") in WATCHLIST:
                keys[row["trading_symbol"]] = row["instrument_key"]
    return keys

def ingest_ticks():
    keys = resolve_instrument_keys()
    today = datetime.date.today().isoformat()
    week_ago = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    for symbol, instrument_key in keys.items():
        data = get_historical_candles(instrument_key, "minutes", "30", today, week_ago)
        data["_symbol"] = symbol
        data["_ingested_at"] = datetime.datetime.utcnow().isoformat()
        with open(f"{VOL_TICKS}/{symbol}_{ts}.json", "w") as f:
            json.dump(data, f)

def ingest_holdings():
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    data = get_holdings()
    with open(f"{VOL_HOLDINGS}/holdings_{ts}.json", "w") as f:
        json.dump(data, f)

def ingest_orders():
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    data = get_order_book()
    with open(f"{VOL_ORDERS}/orders_{ts}.json", "w") as f:
        json.dump(data, f)

if __name__ == "__main__":
    ingest_ticks()
    ingest_holdings()
    ingest_orders()
    print("Ingestion cycle complete.")