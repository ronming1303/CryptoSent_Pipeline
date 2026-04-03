"""
Assemble all raw sources into components.csv (persistent, committed to git),
then run the two-stage Baker-Wurgler PCA to produce CryptoSent and ΔCryptoSent.

Outputs:
  data/processed/components.csv  — raw proxy values (all history, no normalization)
  data/processed/cryptosent.csv  — normalized + PCA-derived index
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from pathlib import Path
from config import RAW_DATA_DIR, PROCESSED_DATA_DIR


# ── Step 1: load historical components + merge today's new raw files ──────────

def load_and_update_components() -> pd.DataFrame:
    """
    Load historical raw proxy values from components.csv (committed to git).
    Merge in any new daily raw files fetched today, then save the updated
    components.csv so the history grows with each run.

    On first run (no components.csv yet), falls back to reading all raw/
    files including backfill — this self-initialises the file.
    """
    components_path = Path(PROCESSED_DATA_DIR) / "components.csv"
    raw = Path(RAW_DATA_DIR)

    if components_path.exists():
        base = pd.read_csv(components_path)
        print(f"[build_index] loaded {len(base)} rows from components.csv")
        # Only pick up new daily files; backfill is already in components.csv
        patterns = [
            ("coingecko_[0-9]*.csv",     ["date", "crypto_index", "dollarVolume"]),
            ("onchain_[0-9]*.csv",       ["date", "address #", "blockUsd"]),
            ("google_trends_[0-9]*.csv", ["date", "google trends"]),
            ("social_[0-9]*.csv",        ["date", "total count"]),
        ]
    else:
        # First-time initialisation: read everything in raw/ (incl. backfill)
        base = pd.DataFrame()
        patterns = [
            ("coingecko_*.csv",     ["date", "crypto_index", "dollarVolume"]),
            ("onchain_*.csv",       ["date", "address #", "blockUsd"]),
            ("google_trends_*.csv", ["date", "google trends"]),
            ("social_*.csv",        ["date", "total count"]),
        ]

    # Merge each source's new files into a single new-rows DataFrame
    new_data = None
    for pattern, cols in patterns:
        files = sorted(raw.glob(pattern))
        if not files:
            continue
        df = pd.concat(
            [pd.read_csv(f)[cols] for f in files], ignore_index=True
        ).drop_duplicates("date")
        new_data = df if new_data is None else new_data.merge(df, on="date", how="outer")

    if new_data is not None and not new_data.empty:
        base = (
            pd.concat([base, new_data], ignore_index=True)
            .drop_duplicates("date", keep="last")
        )

    base = base.sort_values("date").reset_index(drop=True)
    base.to_csv(components_path, index=False)
    print(f"[build_index] components.csv saved: {len(base)} rows → {components_path}")
    return base


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Compute volatility and normalize all proxies to 0-100."""
    # volatility = 30-day rolling std of raw crypto_index
    df["volatility"] = df["crypto_index"].rolling(30).std()
    df = df.dropna(subset=["volatility"]).reset_index(drop=True)
    df["volatility"] = df["volatility"] / df["volatility"].max() * 100

    for col in ["crypto_index", "dollarVolume", "address #", "blockUsd",
                "google trends", "total count"]:
        col_max = df[col].max()
        if col_max and col_max != 0:
            df[col] = df[col] / col_max * 100

    # Google Trends is weekly — forward-fill gaps
    df["google trends"] = df["google trends"].ffill()
    df.fillna(0, inplace=True)
    return df


# ── Step 2: Stage-1 PCA — determine lead/lag for each proxy ──────────────────

def select_lead_lag(df: pd.DataFrame):
    """
    Add lag columns, run PCA on all 16 (current + lag), correlate with PC1,
    pick whichever direction has higher |correlation| for each proxy.
    """
    proxies = ["crypto_index", "volatility", "google trends", "total count",
               "dollarVolume", "address #", "blockUsd"]

    for p in proxies:
        df[f"{p}_lag"] = df[p].shift(7)
    df.fillna(0, inplace=True)

    all_cols = proxies + [f"{p}_lag" for p in proxies]
    scaled = StandardScaler().fit_transform(df[all_cols])
    pc1 = PCA(n_components=1).fit_transform(scaled).flatten()

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
    scaled = StandardScaler().fit_transform(df[cols])

    pca2 = PCA(n_components=1)
    df["CryptoSent"] = pca2.fit_transform(scaled).flatten()
    df["DeltaCryptoSent"] = df["CryptoSent"].diff()

    loadings = pd.DataFrame(pca2.components_.T, index=cols, columns=["loading"])
    print("\n[build_index] Stage-2 PCA loadings:")
    print(loadings.to_string())
    print(f"\n[build_index] Explained variance ratio: {pca2.explained_variance_ratio_[0]:.3f}")

    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    df = load_and_update_components()
    df = normalize(df)
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
    print(f"\n[build_index] cryptosent.csv saved: {len(out)} rows → {out_path}")
    return out


if __name__ == "__main__":
    run()
