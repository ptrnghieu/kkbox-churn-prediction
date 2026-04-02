"""
Sample script: retrieve features from the online store (Redis) for a set of regions.

Run after:
  1. All ingestion + transform scripts have produced gold parquets
  2. `feast apply` has been run inside feature_repo/
  3. `feast materialize-incremental <end_date>` has populated Redis

Usage:
  cd feature_repo/
  python sample_retrieval.py
"""

from feast import FeatureStore


def main():
    store = FeatureStore(repo_path=".")

    ENTITY_ROWS = [
        {"region_id": "ca"},
        {"region_id": "ny"},
        {"region_id": "tx"},
        {"region_id": "fl"},
        {"region_id": "il"},
    ]

    print("=" * 60)
    print("ILINet signals (weekly CDC clinical surveillance)")
    print("=" * 60)
    ilinet_features = store.get_online_features(
        features=[
            "ilinet_signals:weekly_ili_cases",
            "ilinet_signals:weighted_ili_pct",
            "ilinet_signals:ili_pct",
        ],
        entity_rows=ENTITY_ROWS,
    ).to_df()
    print(ilinet_features.to_string())

    print("\n" + "=" * 60)
    print("ILINet trend features (outbreak detection)")
    print("=" * 60)
    trend_features = store.get_online_features(
        features=[
            "ilinet_trend_features:rolling_4wk_avg",
            "ilinet_trend_features:outbreak_risk_score",
            "ilinet_trend_features:outbreak_flag",
            "ilinet_trend_features:is_flu_season",
        ],
        entity_rows=ENTITY_ROWS,
    ).to_df()
    print(trend_features.to_string())

    print("\n" + "=" * 60)
    print("COVIDcast signals (daily multi-source surveillance)")
    print("=" * 60)
    covidcast_features = store.get_online_features(
        features=[
            "covidcast_signals:chng_smoothed_outpatient_flu",
            "covidcast_signals:doctor_visits_smoothed_adj_cli",
            "covidcast_signals:fb_survey_smoothed_hh_cmnty_cli",
        ],
        entity_rows=ENTITY_ROWS,
    ).to_df()
    print(covidcast_features.to_string())

    print("\n" + "=" * 60)
    print("FluSurv-NET hospitalization rates (CDC, HHS region → state)")
    print("=" * 60)
    flusurv_features = store.get_online_features(
        features=[
            "flusurv_signals:rate_overall",
            "flusurv_signals:rate_age_4",
            "flusurv_signals:rolling_4wk_avg_hosp_rate",
            "flusurv_signals:week_over_week_pct_change_hosp",
        ],
        entity_rows=ENTITY_ROWS,
    ).to_df()
    print(flusurv_features.to_string())

    print("\n" + "=" * 60)
    print("NOAA weather (state-level daily averages)")
    print("=" * 60)
    noaa_features = store.get_online_features(
        features=[
            "noaa_signals:tavg",
            "noaa_signals:prcp",
            "noaa_signals:heating_degree_days",
            "noaa_signals:is_flu_season",
        ],
        entity_rows=ENTITY_ROWS,
    ).to_df()
    print(noaa_features.to_string())


if __name__ == "__main__":
    main()
