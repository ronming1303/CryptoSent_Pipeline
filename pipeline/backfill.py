"""
Backfill ~2 years of historical data for all pipeline sources.
Run once to populate data/raw/ before starting daily updates.

Saves one CSV per source (e.g. coingecko_backfill.csv) — these are
automatically picked up by build_index.py's existing glob patterns.
"""

import requests
import pandas as pd
import time
from datetime import datetime, timedelta, date
from pathlib import Path
from pytrends.request import TrendReq
from config import RAW_DATA_DIR, GOOGLE_TRENDS_KEYWORD

DAYS_BACK = 730
RAW = Path(RAW_DATA_DIR)


# ── CoinGecko: BTC market chart as proxy for crypto_index + dollarVolume ──────
# /global/market_cap_chart requires Pro; BTC market_chart is free.
# build_index.py normalizes everything to 0-100, so BTC price/volume as proxy
# is sufficient for the PCA to work correctly.

def _fetch_btc_chunk(days: int) -> pd.DataFrame:
    r = requests.get(
        "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart",
        params={"vs_currency": "usd", "days": days, "interval": "daily"},
        timeout=60,
    )
    r.raise_for_status()
    raw = r.json()
    price_df = pd.DataFrame(raw["prices"],        columns=["ts", "crypto_index"])
    vol_df   = pd.DataFrame(raw["total_volumes"], columns=["ts", "dollarVolume"])
    df = price_df.merge(vol_df, on="ts")
    df["date"] = pd.to_datetime(df["ts"], unit="ms").dt.strftime("%Y-%m-%d")
    return df[["date", "crypto_index", "dollarVolume"]]


def backfill_coingecko():
    print("▶ CoinGecko (crypto_index + dollarVolume via BTC market chart)...")

    # Free tier max is 365 days
    df = _fetch_btc_chunk(365).drop_duplicates("date").sort_values("date").reset_index(drop=True)

    out = RAW / "coingecko_backfill.csv"
    df.to_csv(out, index=False)
    print(f"  saved {len(df)} rows → {out}\n")


# ── Blockchain.com: unique addresses + estimated tx volume USD ────────────────

def backfill_onchain():
    print("▶ Blockchain.com (address # + blockUsd)...")

    r1 = requests.get(
        "https://api.blockchain.info/charts/n-unique-addresses",
        params={"timespan": "2years", "format": "json", "sampled": "false"},
        timeout=60,
    )
    r1.raise_for_status()
    time.sleep(2)

    r2 = requests.get(
        "https://api.blockchain.info/charts/estimated-transaction-volume-usd",
        params={"timespan": "2years", "format": "json", "sampled": "false"},
        timeout=60,
    )
    r2.raise_for_status()

    addr_df = pd.DataFrame(r1.json()["values"])
    addr_df["date"] = pd.to_datetime(addr_df["x"], unit="s").dt.strftime("%Y-%m-%d")
    addr_df = addr_df.rename(columns={"y": "address #"})[["date", "address #"]]

    vol_df = pd.DataFrame(r2.json()["values"])
    vol_df["date"] = pd.to_datetime(vol_df["x"], unit="s").dt.strftime("%Y-%m-%d")
    vol_df = vol_df.rename(columns={"y": "blockUsd"})[["date", "blockUsd"]]

    df = addr_df.merge(vol_df, on="date").drop_duplicates("date").sort_values("date")

    out = RAW / "onchain_backfill.csv"
    df.to_csv(out, index=False)
    print(f"  saved {len(df)} rows → {out}\n")


# ── Alternative.me Fear & Greed ───────────────────────────────────────────────

def backfill_social():
    print("▶ Alternative.me Fear & Greed...")
    r = requests.get(
        f"https://api.alternative.me/fng/?limit={DAYS_BACK}&format=json",
        timeout=15,
    )
    r.raise_for_status()
    rows = [
        {
            "date": pd.to_datetime(int(e["timestamp"]), unit="s").strftime("%Y-%m-%d"),
            "total count": float(e["value"]),
        }
        for e in r.json()["data"]
    ]
    df = pd.DataFrame(rows).drop_duplicates("date").sort_values("date")

    out = RAW / "social_backfill.csv"
    df.to_csv(out, index=False)
    print(f"  saved {len(df)} rows → {out}\n")


# ── Google Trends: chunked (max 269 days per request for daily granularity) ───

def backfill_google_trends():
    print("▶ Google Trends (chunked into 269-day windows)...")
    pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 30), retries=3, backoff_factor=2)

    end   = date.today()
    start = end - timedelta(days=DAYS_BACK)

    chunks = []
    chunk_end = end
    while chunk_end > start:
        chunk_start = max(chunk_end - timedelta(days=269), start)
        timeframe = f"{chunk_start} {chunk_end}"
        print(f"  fetching {timeframe}...")

        pytrends.build_payload(
            [GOOGLE_TRENDS_KEYWORD], cat=0, timeframe=timeframe, geo="", gprop=""
        )
        df = pytrends.interest_over_time()
        if not df.empty:
            df = (
                df.reset_index()[["date", GOOGLE_TRENDS_KEYWORD]]
                .rename(columns={GOOGLE_TRENDS_KEYWORD: "google trends"})
            )
            df["date"] = df["date"].astype(str)
            chunks.append(df)

        chunk_end = chunk_start - timedelta(days=1)
        time.sleep(5)   # avoid Google rate-limiting

    if not chunks:
        print("  no data returned")
        return

    result = pd.concat(chunks).drop_duplicates("date").sort_values("date")
    out = RAW / "google_trends_backfill.csv"
    result.to_csv(out, index=False)
    print(f"  saved {len(result)} rows → {out}\n")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"{'='*60}")
    print(f"CryptoSent Backfill — {DAYS_BACK} days")
    print(f"{'='*60}\n")

    backfill_coingecko()
    backfill_onchain()
    backfill_social()
    backfill_google_trends()

    print("✓ Backfill complete.")
