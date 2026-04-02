import os
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from epiweeks import Week

ALL_STATES = [
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga",
    "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
    "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj",
    "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc",
    "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy",
]

# Maps FluSurv location code → list of region_ids it covers.
# NY has two catchment areas (Albany + Rochester); both map to "ny" and are averaged.
# All states not listed here fall back to "network_all" (national rate).
LOCATION_TO_STATES: dict[str, list[str]] = {
    "CA": ["ca"], "CO": ["co"], "CT": ["ct"], "GA": ["ga"],
    "MD": ["md"], "MI": ["mi"], "MN": ["mn"], "NM": ["nm"],
    "NY_albany": ["ny"], "NY_rochester": ["ny"],
    "OH": ["oh"], "OR": ["or"], "TN": ["tn"], "UT": ["ut"],
}

RATE_COLS = [
    "rate_overall",
    "rate_age_0",   # 0–4 yrs
    "rate_age_1",   # 5–17 yrs
    "rate_age_2",   # 18–49 yrs
    "rate_age_3",   # 50–64 yrs
    "rate_age_4",   # 65+ yrs
]


def _epiweek_to_date(epiweek: int) -> datetime:
    year = epiweek // 100
    week = epiweek % 100
    w = Week(year, week, system="CDC")
    return datetime(w.startdate().year, w.startdate().month, w.startdate().day, tzinfo=timezone.utc)


def _bronze_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map FluSurv location-level data to state-level rows.

    Strategy:
      - States with direct FluSurv coverage → use their own rate
      - NY → average of NY_albany and NY_rochester
      - All other 36 states → use network_all (national aggregate) rate
    """
    df = df.copy()
    df["event_timestamp"] = df["epiweek"].apply(_epiweek_to_date)
    for col in RATE_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Build national fallback lookup: epiweek → rate values
    national = (
        df[df["location"] == "network_all"]
        .set_index("event_timestamp")[RATE_COLS]
    )

    # Build state-specific lookup: (state, epiweek) → rate values
    # Average NY_albany and NY_rochester for ny
    state_rows = []
    for loc, states in LOCATION_TO_STATES.items():
        loc_df = df[df["location"] == loc][["event_timestamp"] + RATE_COLS].copy()
        for state in states:
            loc_df["region_id"] = state
            state_rows.append(loc_df.copy())

    state_df = (
        pd.concat(state_rows, ignore_index=True)
        .groupby(["region_id", "event_timestamp"])[RATE_COLS]
        .mean()
        .reset_index()
    )

    # For each of the 50 states, use state-specific data if available,
    # otherwise fall back to national rate
    all_rows = []
    covered_states = set(state_df["region_id"].unique())

    for state in ALL_STATES:
        if state in covered_states:
            rows = state_df[state_df["region_id"] == state].copy()
        else:
            rows = national.reset_index().copy()
            rows["region_id"] = state

        all_rows.append(rows)

    silver_df = pd.concat(all_rows, ignore_index=True)
    silver_df["created_timestamp"] = datetime.now(timezone.utc)
    silver_df = silver_df.sort_values(["region_id", "event_timestamp"]).reset_index(drop=True)

    return silver_df[["region_id", "event_timestamp", "created_timestamp"] + RATE_COLS]


def _silver_to_gold(df: pd.DataFrame) -> pd.DataFrame:
    """Compute trend features from state-level FluSurv hospitalization rates."""
    df = df.copy()
    grp = df.groupby("region_id")["rate_overall"]

    df["rolling_4wk_avg_hosp_rate"] = grp.transform(
        lambda x: x.rolling(4, min_periods=1).mean()
    )
    df["week_over_week_pct_change_hosp"] = grp.transform(
        lambda x: x.pct_change().fillna(0).replace([np.inf, -np.inf], 0)
    )

    df["week_of_year"] = df["event_timestamp"].dt.isocalendar().week.astype(int)
    df["is_flu_season"] = df["week_of_year"].apply(lambda w: 1 if (w >= 40 or w <= 20) else 0)

    gold_cols = [
        "region_id", "event_timestamp", "created_timestamp",
        "rate_overall",
        "rate_age_0", "rate_age_1", "rate_age_2", "rate_age_3", "rate_age_4",
        "rolling_4wk_avg_hosp_rate",
        "week_over_week_pct_change_hosp",
        "week_of_year",
        "is_flu_season",
    ]
    return df[gold_cols]


def transform_flusurv(
    input_path: str = "data/raw/flusurv.parquet",
    silver_path: str = "data/silver/flusurv.parquet",
    gold_path: str = "feature_repo/data/flusurv_features.parquet",
) -> pd.DataFrame:
    """
    Bronze → Silver → Gold transformation for FluSurv-NET data.

    Bronze: location-level parquet (14 catchment areas + network_all)
    Silver: state-level parquet (50 states; uncovered states use national rate)
    Gold:   Feast-ready parquet with rolling hospitalization features

    Args:
        input_path:  Path to bronze parquet from ingestion/flusurv.py.
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
    transform_flusurv()
