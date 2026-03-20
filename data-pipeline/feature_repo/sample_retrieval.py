from feast import FeatureStore

store = FeatureStore(repo_path=".")

entity_rows = [
    {"region_id": "ca"},
    {"region_id": "ny"},
    {"region_id": "tx"},
]

df = store.get_online_features(
    entity_rows=entity_rows,
    features=[
        "disease_weekly_signals:weekly_ili_cases",
        "disease_weekly_signals:weighted_ili_pct",
        "disease_trend_features:rolling_4wk_avg",
        "disease_trend_features:outbreak_risk_score",
        "disease_trend_features:outbreak_flag",
    ],
).to_df()

print(df)