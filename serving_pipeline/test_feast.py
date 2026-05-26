from feast import FeatureStore
import pandas as pd

repo_path = "../feature_store"
store = FeatureStore(repo_path=repo_path)

feature_refs = [
            "kkbox_features:city",
            "kkbox_features:bd",
            "kkbox_features:gender",

            "kkbox_features:registered_via",
            "kkbox_features:total_transactions",
            "kkbox_features:total_amount_paid",
            "kkbox_features:avg_amount_paid",
            "kkbox_features:auto_renew_count",
            "kkbox_features:cancel_count",

            "kkbox_features:total_log_days",
            "kkbox_features:total_secs",
            "kkbox_features:avg_daily_secs",
            "kkbox_features:total_num_25",
            "kkbox_features:total_num_50",
            "kkbox_features:total_num_75",
            "kkbox_features:total_num_985",
            "kkbox_features:total_num_100",
            "kkbox_features:total_num_unq",
        ]

# feature_vector = store.get_online_features(
#                     features=feature_refs,
#                     entity_rows=[{"msno": "oqNlV79/6DBRB5riFBJNsGy9h9mLL4kYo9bk78l996Y="}]
#                 ).to_dict()

entity_df = pd.DataFrame({
    "msno": [
        "oqNlV79/6DBRB5riFBJNsGy9h9mLL4kYo9bk78l996Y=",
    ],
    "event_timestamp": pd.to_datetime([
        "2020-01-01",
    ])
})
historical_features = store.get_historical_features(
    features=feature_refs,
    entity_df=entity_df
).to_df()

print(historical_features)