"""
Read-only Upstox MARKET DATA client only.
No profile, holdings, or order endpoints — by design, this project
never touches account-specific data.
"""
import requests

BASE_URL = "https://api.upstox.com/v2"
INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"

def get_historical_candles(instrument_key, unit, interval, to_date, from_date):
    url = f"{BASE_URL}/historical-candle/{instrument_key}/{unit}/{interval}/{to_date}/{from_date}"
    resp = requests.get(url, headers={"Accept": "application/json"})
    resp.raise_for_status()
    return resp.json()

def download_instrument_master(dest_path):
    resp = requests.get(INSTRUMENTS_URL)
    resp.raise_for_status()
    with open(dest_path, "wb") as f:
        f.write(resp.content)