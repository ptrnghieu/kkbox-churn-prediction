import os
import numpy as np
import pandas as pd
from datetime import datetime, timezone

# Internal column names for each signal (safe for parquet/SQL)
SIGNAL_COL_MAP: dict[tuple, str] = {
    ("chng",          "smoothed_outpatient_flu"):  "chng_smoothed_outpatient_flu",
    ("doctor-visits", "smoothed_adj_cli"):          "doctor_visits_smoothed_adj_cli",
    ("fb-survey",     "smoothed_hh_cmnty_cli"):     "fb_survey_smoothed_hh_cmnty_cli",
}


def _bronze_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize state-level COVIDcast signals from long to wide format.

    Since ingestion already fetches at geo_type="state", no county→state
    aggregation is needed here. Steps:
      - Map geo_value (state abbrev) → region_id
      - Parse time_value (int YYYYMMDD) → event_timestamp UTC
      - Map (data_source, signal) → clean column name
      - Pivot from long (one row per signal) to wide (one column per signal)
    """
    df = df.copy()

    df["region_id"] = df["geo_value"].str.lower().str.strip()
    df["event_timestamp"] = pd.to_datetime(df["time_value"].astype(str), format="%Y%m%d", utc=True)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df["col_name"] = df.apply(
        lambda r: SIGNAL_COL_MAP.get((r["data_source"], r["signal"])), axis=1
    )
    df = df.dropna(subset=["col_name"])

    wide = df.pivot_table(
        index=["region_id", "event_timestamp"],
        columns="col_name",
        values="value",
        aggfunc="first",
    ).reset_index()
    wide.columns.name = None

    # Ensure all expected signal columns exist even if data was missing
    for col in SIGNAL_COL_MAP.values():
        if col not in wide.columns:
            wide[col] = np.nan

    wide["created_timestamp"] = datetime.now(timezone.utc)
    wide = wide.sort_values(["region_id", "event_timestamp"]).reset_index(drop=True)
    return wide


def _silver_to_gold(df: pd.DataFrame) -> pd.DataFrame:
    """Add rolling and temporal features to silver COVIDcast data."""
    df = df.copy()

    df["rolling_7day_avg_chng"] = (
        df.groupby("region_id")["chng_smoothed_outpatient_flu"]
        .transform(lambda x: x.rolling(7, min_periods=1).mean())
    )
    df["rolling_7day_avg_dv"] = (
        df.groupby("region_id")["doctor_visits_smoothed_adj_cli"]
        .transform(lambda x: x.rolling(7, min_periods=1).mean())
    )

    df["week_of_year"] = df["event_timestamp"].dt.isocalendar().week.astype(int)
    df["is_flu_season"] = df["week_of_year"].apply(lambda w: 1 if (w >= 40 or w <= 20) else 0)

    gold_cols = [
        "region_id", "event_timestamp", "created_timestamp",
        "chng_smoothed_outpatient_flu",
        "doctor_visits_smoothed_adj_cli",
        "fb_survey_smoothed_hh_cmnty_cli",
        "rolling_7day_avg_chng",
        "rolling_7day_avg_dv",
        "week_of_year",
        "is_flu_season",
    ]
    return df[gold_cols]


def transform_covidcast(
    input_path: str = "data/raw/covidcast.parquet",
    silver_path: str = "data/silver/covidcast.parquet",
    gold_path: str = "feature_repo/data/covidcast_features.parquet",
) -> pd.DataFrame:
    """
    Bronze → Silver → Gold transformation for COVIDcast data.

    Bronze: state-level, long-format parquet (one row per state/date/signal)
    Silver: cleaned, wide-format parquet (one column per signal)
    Gold:   Feast-ready parquet with rolling features

    Args:
        input_path:  Path to bronze parquet from ingestion/covidcast.py.
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
    transform_covidcast()
