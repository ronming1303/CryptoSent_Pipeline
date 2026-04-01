# CryptoSent Pipeline

## 项目背景

本项目来自 `/Volumes/Research/Crypto_Factors_Project/`，目标是将原来手动维护的 CryptoSent 情绪指数构造过程，重构为一个可以**每日自动更新**的 pipeline。

## CryptoSent 是什么

CryptoSent 是仿照 Baker & Wurgler (2006) 的股票情绪指数方法，用两阶段 PCA 构造的加密货币市场情绪指标。

### 构造步骤

**Step 1**：收集 8 个原始代理指标（归一化到 0-100）

| 变量 | 含义 |
|------|------|
| `crypto_index` | 市值加权加密货币综合指数 |
| `volatility` | crypto_index 的 30 日滚动标准差 |
| `google_trends` | Google 搜索趋势 |
| `total_count` | BTC/ETH/DOGE 推文数量 |
| `dollarVolume` | CEX 中心化交易所成交额 |
| `address_num` | BTC+ETH+DOGE+ZCash 活跃钱包地址数 |
| `blockUsd` | BTC+ETH+DOGE 链上区块交易金额 |
| `ico` | ETH ERC-20 新代币发行数（一阶差分） |

**Step 2**：对 8 个指标的当期值 + 1期滞后值（共 16 个变量）做第一阶段 PCA，确定每个指标取当期还是滞后：

```
crypto_index_lag, google_trends, total_count_lag, volatility,
dollarVolume_lag, address_num, blockUsd_lag, ico
```

**Step 3**：对上述选出的 8 个变量 StandardScaler 标准化后，做第二阶段 PCA，取第一主成分 → **CryptoSent**

**Step 4**：`ΔCryptoSent = CryptoSent.diff()`，用于因子构造

## 数据来源（免费）

| 指标 | 来源 |
|------|------|
| `crypto_index`, `dollarVolume` | CoinGecko 免费 API |
| BTC `address_num`, `blockUsd` | Blockchain.com Stats API（无需 key）|
| ETH `address_num`, `blockUsd` | Etherscan 免费 API（需注册 key）|
| `google_trends` | pytrends 库 |
| `ico` | Etherscan API |
| `total_count` | ⚠️ Twitter 已付费，考虑用 BitInfoCharts 爬虫或替代指标 |

## 项目结构

```
CryptoSent_Pipeline/
├── pipeline/
│   ├── config.py          # API key 和全局配置
│   ├── fetch_coingecko.py # CoinGecko 数据获取
│   ├── fetch_onchain.py   # 链上数据（Blockchain.com + Etherscan）
│   ├── fetch_trends.py    # Google Trends
│   ├── fetch_social.py    # 社交媒体数据
│   └── build_sentiment.py # 两阶段 PCA 构造 CryptoSent
├── data/
│   ├── raw/               # 各来源原始数据（不入 git）
│   └── processed/         # 合并后的 component.csv 和最终指数
├── notebooks/             # 分析和可视化
└── CLAUDE.md              # 本文件
```

## 待完成

- [ ] `fetch_coingecko.py`：获取市值加权指数和成交额
- [ ] `fetch_onchain.py`：获取链上地址数和区块交易额
- [ ] `fetch_trends.py`：获取 Google Trends
- [ ] `fetch_social.py`：获取社交媒体数据
- [ ] `build_sentiment.py`：两阶段 PCA 构造最终指数
- [ ] `run_daily.py`：每日更新入口脚本
