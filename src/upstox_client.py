"""
Read-only Upstox API client.
Deliberately exposes ONLY GET-style methods — no place/modify/cancel
order functionality exists anywhere in this file, by design.
"""
import requests
from databricks.sdk.runtime import dbutils

BASE_URL = "https://api.upstox.com/v2"
INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"

def _get_access_token():
    return dbutils.secrets.get(scope="upstox", key="access_token")

def _headers():
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {_get_access_token()}",
    }

def get_profile():
    resp = requests.get(f"{BASE_URL}/user/profile", headers=_headers())
    resp.raise_for_status()
    return resp.json()

def get_holdings():
    resp = requests.get(f"{BASE_URL}/portfolio/long-term-holdings", headers=_headers())
    resp.raise_for_status()
    return resp.json()

def get_order_book():
    resp = requests.get(f"{BASE_URL}/order/retrieve-all", headers=_headers())
    resp.raise_for_status()
    return resp.json()

def get_historical_candles(instrument_key, unit, interval, to_date, from_date):
    url = f"{BASE_URL}/historical-candle/{instrument_key}/{unit}/{interval}/{to_date}/{from_date}"
    resp = requests.get(url, headers={"Accept": "application/json"})
    resp.raise_for_status()
    return resp.json()

def download_instrument_master(dest_path):
    """Downloads the daily instrument master (gzipped CSV) to a Volume path."""
    resp = requests.get(INSTRUMENTS_URL)
    resp.raise_for_status()
    with open(dest_path, "wb") as f:
        f.write(resp.content)