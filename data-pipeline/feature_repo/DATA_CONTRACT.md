# Data Contract — Disease Trend Detection Feature Store

This document is the authoritative reference for the model training team.
It describes every feature available in the Feast feature store, how to retrieve them,
and known data quality caveats.

---

## Entity

| Field | Value |
|---|---|
| Name | `region_id` |
| Type | `STRING` |
| Description | 2-letter lowercase US state code |
| Examples | `"ca"`, `"ny"`, `"tx"`, `"fl"` |
| Coverage | All 50 US states |

All feature views join on `region_id`. Always pass lowercase codes.

---

## Target Label

| Field | Feature view | Type | Definition |
|---|---|---|---|
| `outbreak_flag` | `ilinet_trend_features` | `int64` (0 or 1) | 1 if `weekly_ili_cases` > (mean + 2σ) within the state's historical distribution |

**Class imbalance warning:** Outbreaks are rare. Expect ~5–10% positive rate depending on state and season.
Use class weights or SMOTE when training classifiers.

---

## Feature Views

### 1. `ilinet_signals` — Weekly CDC clinical surveillance
**Source:** CDC ILINet via Delphi Epidata API  
**Granularity:** Weekly, state-level  
**Date range:** 2010-10-03 → present  
**TTL:** 52 weeks  
**Update frequency:** Weekly (CDC publishes each Friday)

| Feature | Type | Description |
|---|---|---|
| `weekly_ili_cases` | int64 | Raw count of ILI cases reported that week |
| `total_patients_seen` | int64 | Total outpatient visits reported by providers |
| `provider_count` | int64 | Number of reporting healthcare providers |
| `weighted_ili_pct` | float64 | ILI % weighted by state population (CDC official metric) |
| `ili_pct` | float64 | Raw unweighted ILI percentage |
| `reporting_lag_days` | int64 | Days between epiweek end and data availability |

---

### 2. `ilinet_trend_features` — Engineered outbreak detection features
**Source:** Derived from `ilinet_signals`  
**Granularity:** Weekly, state-level  
**Date range:** 2010-10-03 → present  
**TTL:** 104 weeks

| Feature | Type | Description |
|---|---|---|
| `rolling_4wk_avg` | float64 | 4-week rolling mean of `weekly_ili_cases` |
| `rolling_4wk_std` | float64 | 4-week rolling standard deviation of `weekly_ili_cases` |
| `week_over_week_pct_change` | float64 | % change in `weekly_ili_cases` vs previous week |
| `outbreak_risk_score` | float64 | Z-score of current cases vs historical mean, normalized to [0, 1] |
| `outbreak_flag` | int64 | **Target label.** 1 if cases > mean + 2σ, else 0 |
| `week_of_year` | int64 | ISO week number (1–53) |
| `is_flu_season` | int64 | 1 if week 40–53 or week 1–20 (Oct–Mar), else 0 |
| `week_sin` | float64 | Sine encoding of week_of_year for cyclical seasonality |
| `week_cos` | float64 | Cosine encoding of week_of_year for cyclical seasonality |

---

### 3. `covidcast_signals` — Multi-source digital surveillance
**Source:** Delphi COVIDcast API (CHNG claims, doctor visits, Facebook survey)  
**Granularity:** Daily, state-level  
**Date range:** 2020-03-01 → present  
**TTL:** 90 days  
**Update frequency:** Daily

| Feature | Type | Description |
|---|---|---|
| `chng_smoothed_outpatient_flu` | float64 | % outpatient visits with flu diagnosis (CHNG claims, 7-day smoothed) |
| `doctor_visits_smoothed_adj_cli` | float64 | % outpatient visits with CLI symptoms (smoothed, adjusted) |
| `fb_survey_smoothed_hh_cmnty_cli` | float64 | % households reporting CLI in their community (Facebook survey) |
| `rolling_7day_avg_chng` | float64 | 7-day rolling mean of `chng_smoothed_outpatient_flu` |
| `rolling_7day_avg_dv` | float64 | 7-day rolling mean of `doctor_visits_smoothed_adj_cli` |
| `week_of_year` | int64 | ISO week number |
| `is_flu_season` | int64 | 1 if flu season (Oct–Mar), else 0 |

> **Coverage caveat:** COVIDcast data starts March 2020. Features will be `null` for any
> point-in-time query before that date. If training on pre-2020 data, drop these features
> or impute with zeros.

---

### 4. `flusurv_signals` — CDC lab-confirmed flu hospitalization rates
**Source:** CDC FluSurv-NET via Delphi Epidata API  
**Granularity:** Weekly, state-level  
**Date range:** 2010-01-03 → present  
**TTL:** 104 weeks  
**Update frequency:** Weekly

| Feature | Type | Description |
|---|---|---|
| `rate_overall` | float64 | Overall flu hospitalization rate per 100,000 population |
| `rate_age_0` | float64 | Hospitalization rate: 0–4 years |
| `rate_age_1` | float64 | Hospitalization rate: 5–17 years |
| `rate_age_2` | float64 | Hospitalization rate: 18–49 years |
| `rate_age_3` | float64 | Hospitalization rate: 50–64 years |
| `rate_age_4` | float64 | Hospitalization rate: 65+ years |
| `rolling_4wk_avg_hosp_rate` | float64 | 4-week rolling mean of `rate_overall` |
| `week_over_week_pct_change_hosp` | float64 | % change in `rate_overall` vs previous week |
| `week_of_year` | int64 | ISO week number |
| `is_flu_season` | int64 | 1 if flu season (Oct–Mar), else 0 |

> **Geographic caveat:** FluSurv-NET physically monitors only 14 state catchment areas
> (CA, CO, CT, GA, MD, MI, MN, NM, NY, OH, OR, TN, UT). The remaining 36 states use
> the national aggregate rate (`network_all`) as a fallback. Treat hospitalization features
> for non-covered states as approximate national signals, not state-specific measurements.

---

### 5. `noaa_signals` — Daily weather at state capitals
**Source:** Open-Meteo historical archive API  
**Granularity:** Daily, state-level (state capital coordinates)  
**Date range:** 2018-01-01 → present  
**TTL:** 365 days  
**Update frequency:** Daily

| Feature | Type | Description |
|---|---|---|
| `tmax` | float64 | Maximum daily temperature (°C) |
| `tmin` | float64 | Minimum daily temperature (°C) |
| `tavg` | float64 | Average daily temperature: (tmax + tmin) / 2 (°C) |
| `prcp` | float64 | Total daily precipitation (mm) |
| `rolling_7day_avg_tmax` | float64 | 7-day rolling mean of tmax |
| `rolling_7day_avg_tmin` | float64 | 7-day rolling mean of tmin |
| `rolling_7day_avg_prcp` | float64 | 7-day rolling mean of precipitation |
| `heating_degree_days` | float64 | max(0, 18.3°C − tavg) — cold weather proxy, flu season correlate |
| `cooling_degree_days` | float64 | max(0, tavg − 18.3°C) — hot weather proxy |
| `week_of_year` | int64 | ISO week number |
| `is_flu_season` | int64 | 1 if flu season (Oct–Mar), else 0 |

> **Coverage caveat:** Weather data starts 2018. Features will be `null` for queries
> before that date.
>
> **Granularity caveat:** One measurement point per state (capital city). Weather can
> vary significantly within large states (e.g. TX, CA). This is a limitation of the
> current data collection approach.

---

## How to Retrieve Features

### Setup
```bash
# From data-pipeline/
pip install -r requirements.txt

# Pull latest data from DVC remote (MinIO)
dvc pull

# Make sure Redis is running (online store)
docker run -d -p 6379:6379 redis:7

# Apply feature store registry
cd feature_repo/
feast apply
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")
```

### Online retrieval (latest values from Redis)
```python
from feast import FeatureStore

store = FeatureStore(repo_path="feature_repo/")

features = store.get_online_features(
    features=[
        "ilinet_trend_features:rolling_4wk_avg",
        "ilinet_trend_features:outbreak_risk_score",
        "ilinet_trend_features:outbreak_flag",
        "ilinet_trend_features:is_flu_season",
        "flusurv_signals:rate_overall",
        "covidcast_signals:chng_smoothed_outpatient_flu",
        "noaa_signals:heating_degree_days",
    ],
    entity_rows=[
        {"region_id": "ca"},
        {"region_id": "ny"},
        {"region_id": "tx"},
    ],
).to_df()
```

### Offline retrieval (historical, point-in-time correct)
```python
import pandas as pd
from feast import FeatureStore

store = FeatureStore(repo_path="feature_repo/")

# Define the entity DataFrame with timestamps for point-in-time joins
entity_df = pd.DataFrame({
    "region_id": ["ca", "ny", "tx", "fl", "il"] * 10,
    "event_timestamp": pd.date_range("2023-01-01", periods=50, freq="W", tz="UTC"),
})

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "ilinet_signals:weighted_ili_pct",
        "ilinet_trend_features:rolling_4wk_avg",
        "ilinet_trend_features:outbreak_risk_score",
        "ilinet_trend_features:week_sin",
        "ilinet_trend_features:week_cos",
        "ilinet_trend_features:outbreak_flag",       # target label
        "flusurv_signals:rate_overall",
        "flusurv_signals:rolling_4wk_avg_hosp_rate",
        "covidcast_signals:chng_smoothed_outpatient_flu",
        "covidcast_signals:doctor_visits_smoothed_adj_cli",
        "noaa_signals:tavg",
        "noaa_signals:heating_degree_days",
    ],
).to_df()

# training_df is now ready for model training
X = training_df.drop(columns=["outbreak_flag", "event_timestamp"])
y = training_df["outbreak_flag"]
```

---

## Recommended Feature Sets for Model Training

### Minimal (ILINet only, full date range back to 2010)
```python
features = [
    "ilinet_signals:weighted_ili_pct",
    "ilinet_trend_features:rolling_4wk_avg",
    "ilinet_trend_features:rolling_4wk_std",
    "ilinet_trend_features:week_over_week_pct_change",
    "ilinet_trend_features:outbreak_risk_score",
    "ilinet_trend_features:week_sin",
    "ilinet_trend_features:week_cos",
    "ilinet_trend_features:is_flu_season",
]
```

### Full (all sources, date range constrained to 2020–present by COVIDcast)
```python
features = [
    # ILINet
    "ilinet_signals:weighted_ili_pct",
    "ilinet_trend_features:rolling_4wk_avg",
    "ilinet_trend_features:outbreak_risk_score",
    "ilinet_trend_features:week_sin",
    "ilinet_trend_features:week_cos",
    "ilinet_trend_features:is_flu_season",
    # FluSurv
    "flusurv_signals:rate_overall",
    "flusurv_signals:rate_age_4",              # 65+ most clinically relevant
    "flusurv_signals:rolling_4wk_avg_hosp_rate",
    # COVIDcast
    "covidcast_signals:chng_smoothed_outpatient_flu",
    "covidcast_signals:doctor_visits_smoothed_adj_cli",
    # Weather
    "noaa_signals:tavg",
    "noaa_signals:heating_degree_days",
    "noaa_signals:rolling_7day_avg_prcp",
]
```

---

## Known Limitations

| Issue | Impact | Mitigation |
|---|---|---|
| COVIDcast starts 2020 | Full-feature training set limited to ~5 years | Use minimal feature set for longer history |
| FluSurv covers only 14 states directly | 36 states use national fallback | Flag covered vs non-covered states as a feature; consider dropping for state-specific models |
| NOAA measured at state capital only | Inaccurate for large/diverse states (TX, CA, MT) | Acceptable for state-level prediction; address with multi-point weather in cloud deployment |
| Weekly ILINet vs daily COVIDcast/NOAA | Feast point-in-time join uses the most recent value within TTL | Features are aligned at the weekly boundary during offline retrieval |
| `outbreak_flag` based on historical mean+2σ | Threshold computed on full historical data (data leakage risk) | Recompute threshold on training set only before final model evaluation |
