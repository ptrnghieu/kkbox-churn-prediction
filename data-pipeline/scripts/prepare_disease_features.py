import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import os

STATES = [
    "al","ak","az","ar","ca","co","ct","de","fl","ga",
    "hi","id","il","in","ia","ks","ky","la","me","md",
    "ma","mi","mn","ms","mo","mt","ne","nv","nh","nj",
    "nm","ny","nc","nd","oh","ok","or","pa","ri","sc",
    "sd","tn","tx","ut","vt","va","wa","wv","wi","wy"
]

def fetch_raw_data(start_epiweek="201001", end_epiweek="202452"):
    """Fetch raw ILINet data from Delphi Epidata API."""
    resp = requests.get(
        "https://api.delphi.cmu.edu/epidata/fluview/",
        params={
            "regions": ",".join(STATES),
            "epiweeks": f"{start_epiweek}-{end_epiweek}"
        }
    )
    data = resp.json()
    assert data["result"] == 1, f"API error: {data['message']}"
    df = pd.DataFrame(data["epidata"])
    print(f"Fetched raw data: {df.shape}")
    return df


def epiweek_to_date(epiweek: int) -> datetime:
    """Convert YYYYWW epiweek integer to the Monday of that CDC week."""
    year = epiweek // 100
    week = epiweek % 100
    jan1 = datetime(year, 1, 1, tzinfo=timezone.utc)  # ← add tzinfo here
    first_sunday = jan1 + timedelta(days=(6 - jan1.weekday() + 1) % 7)
    return first_sunday + timedelta(weeks=week - 1)


def prepare_disease_features(
    input_df: pd.DataFrame,
    output_path: str = "feature_repo/data/disease_features.parquet" 
) -> pd.DataFrame:
    """
    Transform raw ILINet API data into Feast-compatible feature Parquet.
    Mirrors the blueprint's prepare_feast_data.py pattern exactly.
    """
    df = input_df.copy()

    # 1. Drop columns we don't need
    df = df.drop(columns=["release_date", "issue",
                           "num_age_0", "num_age_1", "num_age_2",
                           "num_age_3", "num_age_4", "num_age_5"])

    # 2. Rename raw columns → clean feature names
    df = df.rename(columns={
        "region":        "region_id",
        "num_ili":       "weekly_ili_cases",
        "num_patients":  "total_patients_seen",
        "num_providers": "provider_count",
        "wili":          "weighted_ili_pct",
        "ili":           "ili_pct",
        "lag":           "reporting_lag_days",
    })

    # 3. Add Feast-required timestamp columns
    df["event_timestamp"] = df["epiweek"].apply(epiweek_to_date)
    df["created_timestamp"] = datetime.now(timezone.utc)
    df = df.drop(columns=["epiweek"])

    # 4. Sort before computing rolling features
    df = df.sort_values(["region_id", "event_timestamp"]).reset_index(drop=True)

    # 5. Engineer trend features (grouped by region)
    grp = df.groupby("region_id")["weekly_ili_cases"]

    df["rolling_4wk_avg"] = grp.transform(
        lambda x: x.rolling(4, min_periods=1).mean()
    )
    df["rolling_4wk_std"] = grp.transform(
        lambda x: x.rolling(4, min_periods=1).std().fillna(0)
    )
    df["week_over_week_pct_change"] = grp.transform(
        lambda x: x.pct_change().fillna(0).replace([np.inf, -np.inf], 0)
    )

    # 6. Compute outbreak risk score (z-score normalized 0–1)
    region_mean = grp.transform("mean")
    region_std  = grp.transform("std").fillna(1).replace(0, 1)
    df["outbreak_risk_score"] = (
        (df["weekly_ili_cases"] - region_mean) / region_std
    ).clip(0, 5) / 5

    # 7. Derive binary target label: 1 if cases > mean + 2*std
    df["outbreak_flag"] = (
        df["weekly_ili_cases"] > region_mean + 2 * region_std
    ).astype(int)

    # 8. Final column order
    feast_cols = [
        "region_id",
        "event_timestamp",
        "created_timestamp",
        "weekly_ili_cases",
        "total_patients_seen",
        "provider_count",
        "weighted_ili_pct",
        "ili_pct",
        "reporting_lag_days",
        "rolling_4wk_avg",
        "rolling_4wk_std",
        "week_over_week_pct_change",
        "outbreak_risk_score",
        "outbreak_flag",
    ]
    df = df[feast_cols]

    # 9. Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Saved: {output_path}")
    print(f"Shape: {df.shape}")
    print(f"Outbreak weeks: {df['outbreak_flag'].sum()} "
          f"({df['outbreak_flag'].mean():.1%} of rows)")
    print(df.head())
    return df


if __name__ == "__main__":
    raw_df = fetch_raw_data(start_epiweek="201001", end_epiweek="202452")
    df = prepare_disease_features(raw_df)