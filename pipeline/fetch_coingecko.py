"""
Fetch crypto_index (market-cap-weighted index) and dollarVolume from CoinGecko free API.
Outputs: data/raw/coingecko_YYYY-MM-DD.csv
"""

import requests
import pandas as pd
import time
from datetime import date
from config import TOP_N_CRYPTOS, COINGECKO_RATE_LIMIT, RAW_DATA_DIR


def fetch_markets(top_n=TOP_N_CRYPTOS) -> pd.DataFrame:
    """Fetch top-N coins by market cap."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    all_coins = []
    per_page = 250
    pages = (top_n // per_page) + 1

    for page in range(1, pages + 1):
        resp = requests.get(url, params={
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": False,
        }, timeout=30)
        resp.raise_for_status()
        all_coins.extend(resp.json())
        time.sleep(60 / COINGECKO_RATE_LIMIT)

    df = pd.DataFrame(all_coins)[["id", "symbol", "market_cap", "current_price", "total_volume"]]
    return df.dropna(subset=["market_cap"]).head(top_n)


def compute_crypto_index(df: pd.DataFrame) -> dict:
    """Market-cap-weighted price index (base = sum of market caps)."""
    total_mcap = df["market_cap"].sum()
    total_volume = df["total_volume"].sum()
    # Weighted average price (normalized to market cap share)
    crypto_index = (df["current_price"] * df["market_cap"]).sum() / total_mcap
    return {
        "date": str(date.today()),
        "crypto_index": crypto_index,
        "dollarVolume": total_volume,
        "total_market_cap": total_mcap,
    }


def fetch_global_volume() -> float:
    """Fetch total 24h USD volume from CoinGecko /global."""
    resp = requests.get("https://api.coingecko.com/api/v3/global", timeout=30)
    resp.raise_for_status()
    return resp.json()["data"]["total_volume"]["usd"]


def run():
    df = fetch_markets()
    row = compute_crypto_index(df)
    # Also override dollarVolume with global endpoint for completeness
    row["dollarVolume"] = fetch_global_volume()

    out = pd.DataFrame([row])
    out_path = f"{RAW_DATA_DIR}/coingecko_{row['date']}.csv"
    out.to_csv(out_path, index=False)
    print(f"[coingecko] saved → {out_path}")
    return out


if __name__ == "__main__":
    run()
