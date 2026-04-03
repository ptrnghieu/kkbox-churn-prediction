import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from epiweeks import Week


def epiweek_to_date(epiweek: int) -> datetime:
    """Convert YYYYWW integer to the Sunday start of that CDC epiweek (UTC)."""
    year = epiweek // 100
    week = epiweek % 100
    w = Week(year, week, system="CDC")
    return datetime(w.startdate().year, w.startdate().month, w.startdate().day, tzinfo=timezone.utc)


def _bronze_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize raw ILINet API data to canonical silver schema."""
    df = df.copy()

    df = df.drop(columns=[
        "release_date", "issue",
        "num_age_0", "num_age_1", "num_age_2",
        "num_age_3", "num_age_4", "num_age_5",
    ], errors="ignore")  # streaming bronze won't have these batch-only columns

    df = df.rename(columns={
        "region":        "region_id",
        "num_ili":       "weekly_ili_cases",
        "num_patients":  "total_patients_seen",
        "num_providers": "provider_count",
        "wili":          "weighted_ili_pct",
        "ili":           "ili_pct",
        "lag":           "reporting_lag_days",
    })

    df["event_timestamp"] = df["epiweek"].apply(epiweek_to_date)
    df["created_timestamp"] = datetime.now(timezone.utc)
    df = df.drop(columns=["epiweek"])

    df = df.sort_values(["region_id", "event_timestamp"]).reset_index(drop=True)
    return df


def _silver_to_gold(df: pd.DataFrame) -> pd.DataFrame:
    """Engineer trend features from silver ILINet data."""
    df = df.copy()
    grp = df.groupby("region_id")["weekly_ili_cases"]

    df["rolling_4wk_avg"] = grp.transform(lambda x: x.rolling(4, min_periods=1).mean())
    df["rolling_4wk_std"] = grp.transform(lambda x: x.rolling(4, min_periods=1).std().fillna(0))
    df["week_over_week_pct_change"] = grp.transform(
        lambda x: x.pct_change().fillna(0).replace([np.inf, -np.inf], 0)
    )

    region_mean = grp.transform("mean")
    region_std  = grp.transform("std").fillna(1).replace(0, 1)
    df["outbreak_risk_score"] = ((df["weekly_ili_cases"] - region_mean) / region_std).clip(0, 5) / 5
    df["outbreak_flag"] = (df["weekly_ili_cases"] > region_mean + 2 * region_std).astype(int)

    df["week_of_year"] = df["event_timestamp"].dt.isocalendar().week.astype(int)
    df["is_flu_season"] = df["week_of_year"].apply(lambda w: 1 if (w >= 40 or w <= 20) else 0)
    df["week_sin"] = np.sin(2 * np.pi * df["week_of_year"] / 52)
    df["week_cos"] = np.cos(2 * np.pi * df["week_of_year"] / 52)

    gold_cols = [
        "region_id", "event_timestamp", "created_timestamp",
        "weekly_ili_cases", "total_patients_seen", "provider_count",
        "weighted_ili_pct", "ili_pct", "reporting_lag_days",
        "rolling_4wk_avg", "rolling_4wk_std", "week_over_week_pct_change",
        "outbreak_risk_score", "outbreak_flag",
        "week_of_year", "is_flu_season", "week_sin", "week_cos",
    ]
    return df[gold_cols]


def transform_ilinet(
    input_path: str = "data/raw/ilinet.parquet",
    silver_path: str = "data/silver/ilinet.parquet",
    gold_path: str = "feature_repo/data/ilinet_features.parquet",
) -> pd.DataFrame:
    """
    Bronze → Silver → Gold transformation for ILINet data.

    Bronze: raw API parquet (all original columns)
    Silver: cleaned, renamed, timestamp-normalized parquet
    Gold:   Feast-ready parquet with engineered trend features

    Args:
        input_path:  Path to bronze parquet from ingestion/ilinet.py.
        silver_path: Output path for silver (cleaned) parquet.
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
    print(f"Outbreak weeks: {gold_df['outbreak_flag'].sum()} ({gold_df['outbreak_flag'].mean():.1%})")
    return gold_df


if __name__ == "__main__":
    transform_ilinet()
