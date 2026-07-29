"""
Read-only Upstox MARKET DATA client only.
No profile, holdings, or order endpoints — by design, this project
never touches account-specific data.
"""
import requests

BASE_URL_V3 = "https://api.upstox.com/v3"
INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"

def get_historical_candles(instrument_key, unit, interval, to_date, from_date):
    url = f"{BASE_URL_V3}/historical-candle/{instrument_key}/{unit}/{interval}/{to_date}/{from_date}"
    resp = requests.get(url, headers={"Accept": "application/json"})
    resp.raise_for_status()
    return resp.json()

def fetch_instrument_master_bytes():
    """Returns the raw gzipped instrument master bytes — no local disk write."""
    resp = requests.get(INSTRUMENTS_URL)
    resp.raise_for_status()
    return resp.content