"""
Daily update entry point.
Run this script once a day (e.g., via cron at 01:00 UTC) to fetch all sources
and rebuild the CryptoSent index.

Usage:
    python daily_update.py

Cron example (01:00 UTC every day):
    0 1 * * * cd /Volumes/Research/CryptoSent_Pipeline/pipeline && python daily_update.py >> ../logs/daily.log 2>&1
"""

import sys
import traceback
from datetime import datetime

print(f"\n{'='*60}")
print(f"CryptoSent Daily Update — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
print(f"{'='*60}\n")

# Each fetcher is independent — failures don't block the others
import fetch_coingecko
import fetch_onchain
import fetch_google_trends
import fetch_social
import build_index

steps = [
    ("CoinGecko (crypto_index + dollarVolume)", fetch_coingecko.run),
    ("On-chain (address#, blockUsd, ico)",       fetch_onchain.run),
    ("Google Trends",                            fetch_google_trends.run),
    ("Social / Fear & Greed",                    fetch_social.run),
    ("Build CryptoSent index",                   build_index.run),
]

for name, fn in steps:
    print(f"\n▶ {name}")
    try:
        fn()
    except Exception as e:
        print(f"  [ERROR] {e}")
        traceback.print_exc()

print(f"\n✓ Done — {datetime.utcnow().strftime('%H:%M UTC')}")
