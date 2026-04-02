"""
Assemble all raw sources into component_origins.csv, then run the
two-stage Baker-Wurgler PCA to produce CryptoSent and ΔCryptoSent.

Output: data/processed/cryptosent.csv
  columns: date, crypto_index, volatility, google trends, total count,
           dollarVolume, address #, blockUsd, ico,
           CryptoSent, DeltaCryptoSent
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from pathlib import Path
from config import RAW_DATA_DIR, PROCESSED_DATA_DIR


# ── Step 1: load and merge all raw daily CSVs ─────────────────────────────────

def load_raw() -> pd.DataFrame:
    raw = Path(RAW_DATA_DIR)

    coingecko = pd.concat(
        [pd.read_csv(f) for f in sorted(raw.glob("coingecko_*.csv"))],
        ignore_index=True,
    ).drop_duplicates("date")

    onchain = pd.concat(
        [pd.read_csv(f) for f in sorted(raw.glob("onchain_*.csv"))],
        ignore_index=True,
    ).drop_duplicates("date")

    trends = pd.concat(

        [pd.read_csv(f) for f in sorted(raw.glob("google_trends_*.csv"))],
        ignore_index=True,
    ).drop_duplicates("date")

    social = pd.concat(
        [pd.read_csv(f) for f in sorted(raw.glob("social_*.csv"))],
        ignore_index=True,
    ).drop_duplicates("date")

    df = (
        coingecko[["date", "crypto_index", "dollarVolume"]]
        .merge(onchain[["date", "address #", "blockUsd"]], on="date", how="left")
        .merge(trends[["date", "google trends"]], on="date", how="left")
        .merge(social[["date", "total count"]], on="date", how="left")
    )

    df = df.sort_values("date").reset_index(drop=True)

    # volatility = 30-day rolling std of crypto_index (normalized 0-100)
    df["volatility"] = df["crypto_index"].rolling(30).std()
    # Drop the first 29 rows where rolling std is undefined (insufficient history)
    df = df.dropna(subset=["volatility"]).reset_index(drop=True)
    df["volatility"] = df["volatility"] / df["volatility"].max() * 100

    # Normalize all proxies to 0-100
    for col in ["crypto_index", "dollarVolume", "address #", "blockUsd", "google trends", "total count"]:
        col_max = df[col].max()
        if col_max and col_max != 0:
            df[col] = df[col] / col_max * 100

    # Forward-fill Google Trends (weekly updates)
    df["google trends"] = df["google trends"].ffill()
    df.fillna(0, inplace=True)

    return df


# ── Step 2: Stage-1 PCA — determine lead/lag for each proxy ──────────────────

def select_lead_lag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add lag columns, run PCA on all 16 (current + lag), correlate with PC1,
    pick whichever direction has higher |correlation| for each proxy.
    Returns df with the selected columns appended.
    """
    proxies = ["crypto_index", "volatility", "google trends", "total count",
               "dollarVolume", "address #", "blockUsd"]

    for p in proxies:
        df[f"{p}_lag"] = df[p].shift(7)  # 7-day lag (weekly frequency)
    df.fillna(0, inplace=True)

    all_cols = proxies + [f"{p}_lag" for p in proxies]
    scaler = StandardScaler()
    scaled = scaler.fit_transform(df[all_cols])
    pca1 = PCA(n_components=1)
    pc1 = pca1.fit_transform(scaled).flatten()

    # Correlate each proxy (current vs lag) with PC1
    selected = {}
    for p in proxies:
        corr_cur = abs(np.corrcoef(df[p], pc1)[0, 1])
        corr_lag = abs(np.corrcoef(df[f"{p}_lag"], pc1)[0, 1])
        selected[p] = p if corr_cur >= corr_lag else f"{p}_lag"

    print("[build_index] Stage-1 lead/lag selection:")
    for k, v in selected.items():
        print(f"  {k:20s} → {v}")

    return df, selected


# ── Step 3: Stage-2 PCA — final CryptoSent ───────────────────────────────────

def build_cryptosent(df: pd.DataFrame, selected: dict) -> pd.DataFrame:
    cols = list(selected.values())
    scaler = StandardScaler()
    scaled = scaler.fit_transform(df[cols])

    pca2 = PCA(n_components=1)
    df["CryptoSent"] = pca2.fit_transform(scaled).flatten()
    df["DeltaCryptoSent"] = df["CryptoSent"].diff()

    loadings = pd.DataFrame(
        pca2.components_.T, index=cols, columns=["loading"]
    )
    print("\n[build_index] Stage-2 PCA loadings:")
    print(loadings.to_string())
    print(f"\n[build_index] Explained variance ratio: {pca2.explained_variance_ratio_[0]:.3f}")

    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    df = load_raw()
    df, selected = select_lead_lag(df)
    df = build_cryptosent(df, selected)

    out_cols = [
        "date", "crypto_index", "volatility", "google trends", "total count",
        "dollarVolume", "address #", "blockUsd",
        "CryptoSent", "DeltaCryptoSent",
    ]
    out = df[out_cols].dropna(subset=["CryptoSent"])
    out_path = f"{PROCESSED_DATA_DIR}/cryptosent.csv"
    out.to_csv(out_path, index=False)
    print(f"\n[build_index] saved {len(out)} rows → {out_path}")
    return out


if __name__ == "__main__":
    run()
