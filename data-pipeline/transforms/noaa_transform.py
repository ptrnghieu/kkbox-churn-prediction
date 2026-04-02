import os
import numpy as np
import pandas as pd
from datetime import datetime, timezone

BASE_TEMP_C = 18.3  # base temperature for degree-day calculation (≈65°F)


def _bronze_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize Open-Meteo bronze output to state-day silver rows.

    Bronze schema (from ingestion/noaa.py):
      state, date (ISO string), tmax (°C), tmin (°C), prcp (mm)

    Steps:
      - Parse date → event_timestamp UTC
      - Normalise region_id to lowercase 2-letter state code
      - Coerce numeric columns, compute tavg = (tmax + tmin) / 2
    """
    df = df.copy()

    df["event_timestamp"] = pd.to_datetime(df["date"], utc=True)
    df["region_id"]       = df["state"].str.lower().str.strip()

    for col in ["tmax", "tmin", "prcp"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["tavg"] = (df["tmax"] + df["tmin"]) / 2

    df["created_timestamp"] = datetime.now(timezone.utc)
    silver_df = df[["region_id", "event_timestamp", "created_timestamp", "tmax", "tmin", "tavg", "prcp"]]
    return silver_df.sort_values(["region_id", "event_timestamp"]).reset_index(drop=True)


def _silver_to_gold(df: pd.DataFrame) -> pd.DataFrame:
    """Add rolling weather features and degree-day indices."""
    df = df.copy()

    for col in ["tmax", "tmin", "prcp"]:
        roll_col = f"rolling_7day_avg_{col}"
        df[roll_col] = (
            df.groupby("region_id")[col]
            .transform(lambda x: x.rolling(7, min_periods=1).mean())
        )

    # Heating degree days: energy proxy for cold weather (flu season correlate)
    df["heating_degree_days"] = (BASE_TEMP_C - df["tavg"]).clip(lower=0)
    # Cooling degree days: proxy for hot weather
    df["cooling_degree_days"] = (df["tavg"] - BASE_TEMP_C).clip(lower=0)

    df["week_of_year"] = df["event_timestamp"].dt.isocalendar().week.astype(int)
    df["is_flu_season"] = df["week_of_year"].apply(lambda w: 1 if (w >= 40 or w <= 20) else 0)

    gold_cols = [
        "region_id", "event_timestamp", "created_timestamp",
        "tmax", "tmin", "tavg", "prcp",
        "rolling_7day_avg_tmax",
        "rolling_7day_avg_tmin",
        "rolling_7day_avg_prcp",
        "heating_degree_days",
        "cooling_degree_days",
        "week_of_year",
        "is_flu_season",
    ]
    return df[gold_cols]


def transform_noaa(
    input_path: str = "data/raw/noaa.parquet",
    silver_path: str = "data/silver/noaa.parquet",
    gold_path: str = "feature_repo/data/noaa_features.parquet",
) -> pd.DataFrame:
    """
    Bronze → Silver → Gold transformation for NOAA weather data.

    Bronze: wide-format parquet from Open-Meteo (one row per state/date: tmax, tmin, prcp)
    Silver: cleaned, wide-format, state-level daily averages
    Gold:   Feast-ready parquet with rolling weather features

    Feast handles the frequency mismatch between daily NOAA data and
    weekly ILINet data at point-in-time join time via TTL.

    Args:
        input_path:  Path to bronze parquet from ingestion/noaa.py.
        silver_path: Output path for silver parquet.
        gold_path:   Output path for gold (Feast-ready) parquet.

    Returns:
        Gold DataFrame.
    """
    print(f"Loading bronze: {input_path}")
    raw_df = pd.read_parquet(input_path)
    print(f"  Shape: {raw_df.shape}")

    silver_df = _bronze_to_silver(raw_df)
    os.makedirs(os.path.dirname(silver_path), exist_ok=True)
    silver_df.to_parquet(silver_path, index=False)
    print(f"Saved silver → {silver_path}  {silver_df.shape}")

    gold_df = _silver_to_gold(silver_df)
    os.makedirs(os.path.dirname(gold_path), exist_ok=True)
    gold_df.to_parquet(gold_path, index=False)
    print(f"Saved gold   → {gold_path}  {gold_df.shape}")
    return gold_df


if __name__ == "__main__":
    transform_noaa()
