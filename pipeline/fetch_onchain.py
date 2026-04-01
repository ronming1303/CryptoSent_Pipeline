"""
Fetch on-chain data from Blockchain.com Stats API (no API key required):
  - address #  : daily unique BTC addresses used
  - blockUsd   : estimated BTC transaction volume in USD

Outputs: data/raw/onchain_YYYY-MM-DD.csv
"""

import requests
import pandas as pd
from datetime import date
from config import RAW_DATA_DIR


BASE_CHARTS = "https://api.blockchain.info/charts"


def fetch_unique_addresses() -> float:
    """Most recent daily unique BTC addresses."""
    r = requests.get(
        f"{BASE_CHARTS}/n-unique-addresses",
        params={"timespan": "3days", "format": "json", "sampled": "false"},
        timeout=30,
    )
    r.raise_for_status()
    values = r.json()["values"]
    return float(values[-1]["y"])


def fetch_block_usd() -> float:
    """Estimated BTC transaction volume in USD (from /stats)."""
    r = requests.get("https://api.blockchain.info/stats", timeout=30)
    r.raise_for_status()
    return float(r.json().get("estimated_transaction_volume_usd", 0))


def run():
    today = str(date.today())

    row = {
        "date": today,
        "address #": fetch_unique_addresses(),
        "blockUsd": fetch_block_usd(),
    }

    out = pd.DataFrame([row])
    out_path = f"{RAW_DATA_DIR}/onchain_{today}.csv"
    out.to_csv(out_path, index=False)
    print(f"[onchain] saved → {out_path}")
    return out


if __name__ == "__main__":
    run()
