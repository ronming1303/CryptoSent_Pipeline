"""
Fetch Google Trends for 'cryptocurrency' via pytrends (unofficial, free).
Note: Google Trends returns weekly data — we forward-fill for daily pipeline.
Outputs: data/raw/google_trends_YYYY-MM-DD.csv

Install: pip install pytrends
"""

import pandas as pd
from datetime import date, timedelta
from pytrends.request import TrendReq
from config import GOOGLE_TRENDS_KEYWORD, RAW_DATA_DIR


def fetch_trends(keyword: str = GOOGLE_TRENDS_KEYWORD, days_back: int = 30) -> pd.DataFrame:
    """
    Pull the last `days_back` days of daily Google Trends data.
    pytrends returns daily granularity only for timeframes <= 270 days.
    """
    pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 30), retries=3, backoff_factor=2)

    end = date.today()
    start = end - timedelta(days=days_back)
    timeframe = f"{start} {end}"

    pytrends.build_payload([keyword], cat=0, timeframe=timeframe, geo="", gprop="")
    df = pytrends.interest_over_time()

    if df.empty:
        return pd.DataFrame()

    df = df.reset_index()[["date", keyword]].rename(columns={keyword: "google trends"})
    df["date"] = df["date"].astype(str)
    return df


def run():
    today = str(date.today())
    df = fetch_trends()
    if df.empty:
        print("[google_trends] no data returned")
        return None

    out_path = f"{RAW_DATA_DIR}/google_trends_{today}.csv"
    df.to_csv(out_path, index=False)
    print(f"[google_trends] saved {len(df)} rows → {out_path}")
    return df


if __name__ == "__main__":
    run()
