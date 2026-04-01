"""
Fetch social media activity (tweet count proxy).

Twitter official API is now paid. Two free alternatives are implemented:
  1. Alternative.me Fear & Greed Index (fully free, JSON API, no key needed)
  2. BitInfoCharts scraper for BTC/ETH/DOGE tweet counts (fragile, may break)

The pipeline uses Alternative.me by default as it is stable.
Outputs: data/raw/social_YYYY-MM-DD.csv
"""

import requests
import pandas as pd
from datetime import date
from config import RAW_DATA_DIR


# ── Option 1: Alternative.me Fear & Greed Index (recommended) ────────────────

def fetch_fear_greed(limit: int = 1) -> pd.DataFrame:
    """
    Free Fear & Greed Index API.
    Docs: https://alternative.me/crypto/fear-and-greed-index/
    Returns a 0-100 score — we use this as the social/sentiment proxy.
    """
    resp = requests.get(
        f"https://api.alternative.me/fng/?limit={limit}&format=json",
        timeout=15,
    )
    resp.raise_for_status()
    rows = []
    for entry in resp.json()["data"]:
        rows.append({
            "date": pd.to_datetime(int(entry["timestamp"]), unit="s").strftime("%Y-%m-%d"),
            "total count": float(entry["value"]),  # 0-100, same scale as original
        })
    return pd.DataFrame(rows)


# ── Option 2: BitInfoCharts scraper (fallback, fragile) ──────────────────────

def fetch_bitinfocharts_tweets() -> pd.DataFrame:
    """
    Scrape BTC tweet count from BitInfoCharts.
    WARNING: This may break if the site changes its structure.
    """
    import re
    url = "https://bitinfocharts.com/comparison/bitcoin-tweets.html"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    # Extract the data array from the embedded JS
    match = re.search(r'new Dygraph\(.*?\[(\[.*?\])\]', resp.text, re.DOTALL)
    if not match:
        return pd.DataFrame()

    raw = match.group(1)
    pairs = re.findall(r'\["(\d{4}/\d{2}/\d{2})",([\d.]+)\]', raw)
    df = pd.DataFrame(pairs, columns=["date", "total count"])
    df["date"] = pd.to_datetime(df["date"], format="%Y/%m/%d").dt.strftime("%Y-%m-%d")
    df["total count"] = df["total count"].astype(float)
    return df.tail(30)  # last 30 days


# ── Main ──────────────────────────────────────────────────────────────────────

def run(use_fear_greed: bool = True):
    today = str(date.today())
    if use_fear_greed:
        df = fetch_fear_greed(limit=7)  # last 7 days
    else:
        df = fetch_bitinfocharts_tweets()

    if df.empty:
        print("[social] no data returned")
        return None

    out_path = f"{RAW_DATA_DIR}/social_{today}.csv"
    df.to_csv(out_path, index=False)
    print(f"[social] saved {len(df)} rows → {out_path}")
    return df


if __name__ == "__main__":
    run()
