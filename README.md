# CryptoSent Pipeline

Daily pipeline to construct the **CryptoSent** sentiment index for crypto markets,
replicating the Baker & Wurgler (2006) two-stage PCA method.

## Directory Structure

```
CryptoSent_Pipeline/
├── pipeline/
│   ├── config.py              # API keys and settings
│   ├── fetch_coingecko.py     # crypto_index, dollarVolume
│   ├── fetch_onchain.py       # address#, blockUsd, ico  (Blockchain.com + Etherscan)
│   ├── fetch_google_trends.py # google trends            (pytrends)
│   ├── fetch_social.py        # total count              (Alternative.me Fear&Greed)
│   ├── build_index.py         # two-stage PCA → CryptoSent
│   └── daily_update.py        # entry point
├── data/
│   ├── raw/                   # one CSV per source per day
│   └── processed/
│       └── cryptosent.csv     # final output
├── notebooks/                 # analysis notebooks
├── requirements.txt
└── logs/
```

## Setup

```bash
pip install -r requirements.txt
```

Edit `pipeline/config.py`:
- Set `ETHERSCAN_API_KEY` (free at https://etherscan.io/apis)

## Run

```bash
cd pipeline
python daily_update.py
```

## Data Sources

| Component | Source | Free? |
|-----------|--------|-------|
| crypto_index, dollarVolume | CoinGecko API | ✅ Free (30 req/min) |
| volatility | Derived from crypto_index | ✅ |
| google trends | pytrends (unofficial) | ✅ Free |
| total count | Alternative.me Fear & Greed | ✅ Free |
| address # | Blockchain.com + Etherscan | ✅ Free |
| blockUsd | Blockchain.com + Etherscan | ✅ Free |
| ico | Etherscan daily new addresses | ✅ Free |

## Output: cryptosent.csv

| Column | Description |
|--------|-------------|
| date | YYYY-MM-DD |
| crypto_index | Market-cap-weighted price index (0-100) |
| volatility | 30-day rolling std of crypto_index (0-100) |
| google trends | Google search interest (0-100) |
| total count | Fear & Greed proxy (0-100) |
| dollarVolume | CEX total USD volume (0-100) |
| address # | BTC+ETH active addresses (0-100) |
| blockUsd | BTC+ETH on-chain tx value (0-100) |
| ico | New contract deployments (0-100) |
| CryptoSent | Final sentiment index (PCA score) |
| DeltaCryptoSent | First difference of CryptoSent |
