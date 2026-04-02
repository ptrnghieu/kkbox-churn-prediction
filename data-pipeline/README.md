# Data Pipeline

Ingests weekly flu surveillance data from 4 public health APIs, engineers ML-ready
features using the Medallion Architecture (Bronze → Silver → Gold), and registers
them in a Feast feature store backed by Redis (online) and Parquet/MinIO (offline).

---

## Architecture

```
External APIs
    │
    ▼
┌─────────────────────────────────────────────────────┐
│  ingestion/          (Bronze layer)                  │
│  ilinet.py           CDC ILINet weekly signals       │
│  covidcast.py        CMU COVIDcast daily signals     │
│  flusurv.py          CDC FluSurv-NET hospitalizations│
│  noaa.py             Open-Meteo daily weather        │
└────────────────────────┬────────────────────────────┘
                         │ raw parquets → data/raw/
                         ▼
┌─────────────────────────────────────────────────────┐
│  transforms/         (Silver → Gold layers)          │
│  ilinet_transform.py                                 │
│  covidcast_transform.py                              │
│  flusurv_transform.py                                │
│  noaa_transform.py                                   │
└────────────────────────┬────────────────────────────┘
                         │ feature parquets → feature_repo/data/
                         ▼
┌─────────────────────────────────────────────────────┐
│  feature_repo/       (Feast feature store)           │
│  Offline store:  Parquet files (versioned via DVC)   │
│  Online store:   Redis (latest values per state)     │
└─────────────────────────────────────────────────────┘
```

---

## Prerequisites

- Python 3.12+
- Docker (for Redis and MinIO)
- A running MinIO instance (see [../infra/docker/README.md](../infra/docker/README.md))

---

## Setup

```bash
cd data-pipeline
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## Running the Pipeline

All commands run from the `data-pipeline/` directory with the venv activated.

### Option A — Pull existing data from DVC (recommended)

If MinIO is running and configured:
```bash
dvc pull
```

This downloads all raw and gold parquets without re-fetching from the APIs.

### Option B — Run ingestion from scratch

```bash
# Step 1: Fetch raw data from APIs (Bronze layer)
python -m ingestion.ilinet       # ~2 min,  CDC weekly flu data
python -m ingestion.covidcast    # ~1 min,  CMU digital surveillance signals
python -m ingestion.flusurv      # ~1 min,  CDC hospitalization rates
python -m ingestion.noaa         # ~8 min,  Open-Meteo weather (50 states × 6s delay)

# Step 2: Transform to feature-ready format (Silver + Gold layers)
python -m transforms.ilinet_transform
python -m transforms.covidcast_transform
python -m transforms.flusurv_transform
python -m transforms.noaa_transform

# Step 3: Push new data to DVC remote
dvc add data/raw/*.parquet data/silver/*.parquet feature_repo/data/*.parquet
dvc push
```

### Step 3 — Register and populate the feature store

```bash
# Start Redis if not already running
docker run -d --name redis -p 6379:6379 redis:7

# Register feature views with Feast
cd feature_repo/
feast apply

# Populate Redis with latest feature values
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")
```

### Step 4 — Verify

```bash
python feature_repo/sample_retrieval.py
```

You should see feature values printed for CA, NY, TX, FL, IL.

---

## Data Flow Details

### Ingestion (Bronze layer)

Each script fetches raw API data and saves it as-is to `data/raw/`.

| Script | Output | Rows | Notes |
|---|---|---|---|
| `ingestion/ilinet.py` | `data/raw/ilinet.parquet` | ~39K | Weekly, 50 states, 2010→present |
| `ingestion/covidcast.py` | `data/raw/covidcast.parquet` | ~118K | Daily, 50 states, 2020→present |
| `ingestion/flusurv.py` | `data/raw/flusurv.parquet` | ~8K | Weekly, 14 catchment areas + national |
| `ingestion/noaa.py` | `data/raw/noaa.parquet` | ~151K | Daily, 50 state capitals, 2018→present |

### Transforms (Silver → Gold)

Each transform script runs two stages:

1. **Bronze → Silver** (`data/silver/`): cleaning, type parsing, geographic normalization to state level
2. **Silver → Gold** (`feature_repo/data/`): feature engineering (rolling averages, outbreak labels, seasonality encoding)

| Script | Key transformations |
|---|---|
| `ilinet_transform.py` | CDC epiweek → UTC timestamp, rolling_4wk_avg/std, outbreak_flag (mean+2σ), week_sin/cos |
| `covidcast_transform.py` | Long → wide pivot per signal, rolling_7day_avg |
| `flusurv_transform.py` | 14 locations → 50 states (network_all fallback), rolling_4wk_avg_hosp_rate |
| `noaa_transform.py` | TAVG = (TMAX+TMIN)/2, heating/cooling degree days, rolling_7day_avg |

### Feature Store

5 Feast feature views registered in `feature_repo/`:

| Feature view | Source | TTL | Key features |
|---|---|---|---|
| `ilinet_signals` | ILINet | 52 weeks | `weighted_ili_pct`, `ili_pct`, `provider_count` |
| `ilinet_trend_features` | ILINet | 104 weeks | `outbreak_flag` ⭐, `outbreak_risk_score`, `rolling_4wk_avg` |
| `covidcast_signals` | COVIDcast | 90 days | `chng_smoothed_outpatient_flu`, `doctor_visits_smoothed_adj_cli` |
| `flusurv_signals` | FluSurv | 104 weeks | `rate_overall`, `rate_age_4` (65+), `rolling_4wk_avg_hosp_rate` |
| `noaa_signals` | Open-Meteo | 365 days | `tavg`, `heating_degree_days`, `rolling_7day_avg_tmax` |

⭐ `outbreak_flag` in `ilinet_trend_features` is the **target label** for model training.

See [feature_repo/DATA_CONTRACT.md](feature_repo/DATA_CONTRACT.md) for the full feature
specification and retrieval examples.

---

## DVC Data Versioning

Data files are versioned with DVC and stored in MinIO (`disease-processed` bucket).
Git only stores the `.dvc` pointer files.

```bash
# Push new/updated data files
dvc push

# Pull data files after cloning the repo
dvc pull

# Check what's tracked
dvc status
```

DVC remote config is in `.dvc/config`. The MinIO credentials are:
- Endpoint: `http://localhost:9000`
- Access key: `minioadmin`
- Secret key: `minioadmin`
- Bucket: `disease-processed`

---

## Directory Structure

```
data-pipeline/
├── ingestion/
│   ├── __init__.py
│   ├── ilinet.py
│   ├── covidcast.py
│   ├── flusurv.py
│   └── noaa.py
├── transforms/
│   ├── __init__.py
│   ├── ilinet_transform.py
│   ├── covidcast_transform.py
│   ├── flusurv_transform.py
│   └── noaa_transform.py
├── feature_repo/
│   ├── feature_store.yaml      # Feast config (offline=file, online=Redis)
│   ├── disease_entities.py     # region entity (US state code)
│   ├── data_sources.py         # FileSource definitions per source
│   ├── feature_views.py        # 5 FeatureView definitions
│   ├── sample_retrieval.py     # Example online retrieval script
│   └── DATA_CONTRACT.md        # Full feature specification for model team
├── data/
│   ├── raw/                    # Bronze parquets (DVC-tracked)
│   └── silver/                 # Silver parquets (DVC-tracked)
├── .dvc/
│   └── config                  # DVC remote (MinIO)
├── requirements.txt
└── README.md                   # This file
```

---

## Troubleshooting

**`feast apply` fails with Redis connection error**
→ Redis is not running. Start it: `docker run -d -p 6379:6379 redis:7`

**`dvc push` fails with boto3 error**
→ Run: `pip install "aiobotocore[boto3]==2.21.1"`

**NOAA ingestion hits 429 rate limit**
→ The script already backs off 60s on 429. If it keeps failing, wait a few minutes and retry.

**COVIDcast returns no results for a signal**
→ Some signals have coverage gaps. The script skips "no results" responses — check the date range.

**FluSurv returns empty data**
→ Ensure you are using the valid location codes defined in `ingestion/flusurv.py`. HHS region codes (`hhs1`-`hhs10`) are not valid for this API.
