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

def get_current_epiweek() -> str:
    now = datetime.now()
    week = now.isocalendar()[1]
    return f"{now.year}{week:02d}"

def fetch_raw_data(start_epiweek="201001"):
    """Fetch raw ILINet data from Delphi Epidata API."""
    end_epiweek = get_current_epiweek()
    print(f"Fetching {start_epiweek} → {end_epiweek}")
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
    """
    df = input_df.copy()

    #Drop columns we don't need
    df = df.drop(columns=["release_date", "issue",
                           "num_age_0", "num_age_1", "num_age_2",
                           "num_age_3", "num_age_4", "num_age_5"])

    #Rename raw columns → clean feature names
    df = df.rename(columns={
        "region":        "region_id",  #the geographic region
        "num_ili":       "weekly_ili_cases", #the raw number of influenza-like cases reported
        "num_patients":  "total_patients_seen", #the total number of patients seen by providers
        "num_providers": "provider_count", #the total number of providers reporting data
        "wili":          "weighted_ili_pct", #weighted ILI%: weighted by state population for national/regional estimates
        "ili":           "ili_pct", #numili / num_patients, the raw percentage of patients with ILI symptoms
        "lag":           "reporting_lag_days", 
    })

    #Add Feast-required timestamp columns
    df["event_timestamp"] = df["epiweek"].apply(epiweek_to_date)
    df["created_timestamp"] = datetime.now(timezone.utc)
    df = df.drop(columns=["epiweek"])

    #Sort before computing rolling features
    df = df.sort_values(["region_id", "event_timestamp"]).reset_index(drop=True)

    #Engineer trend features (grouped by region)
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

    #Compute outbreak risk score (z-score normalized 0–1)
    region_mean = grp.transform("mean")
    region_std  = grp.transform("std").fillna(1).replace(0, 1)
    df["outbreak_risk_score"] = (
        (df["weekly_ili_cases"] - region_mean) / region_std
    ).clip(0, 5) / 5

    #Derive binary target label: 1 if cases > mean + 2*std
    df["outbreak_flag"] = (
        df["weekly_ili_cases"] > region_mean + 2 * region_std
    ).astype(int)

    df['week_of_year'] = df['event_timestamp'].dt.isocalendar().week.astype(int)
    df['is_flu_season'] = df['week_of_year'].apply(
        lambda w: 1 if (w >= 40 or w <= 20) else 0 #Oct - Mar
    )

    #Cyclical encoding so model understands week 52 and week 1 are close
    df['week_sin'] = np.sin(2 * np.pi * df['week_of_year'] / 52)
    df['week_cos'] = np.cos(2 * np.pi * df['week_of_year'] / 52)

    #Final column order
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
        "week_of_year",
        "is_flu_season",
        "week_sin",
        "week_cos",
    ]
    df = df[feast_cols]

    #Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Saved: {output_path}")
    print(f"Shape: {df.shape}")
    print(f"Outbreak weeks: {df['outbreak_flag'].sum()} "
          f"({df['outbreak_flag'].mean():.1%} of rows)")
    print(df.head())
    return df


if __name__ == "__main__":
    raw_df = fetch_raw_data(start_epiweek="201001")
    df = prepare_disease_features(raw_df)