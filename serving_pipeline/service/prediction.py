import json
import logging
import os
import tempfile

import numpy as np
import pandas as pd
import xgboost as xgb
from app.metrics import FeastFetchTimer
from feast import FeatureStore
from google.cloud import storage as gcs

logger = logging.getLogger(__name__)

GCS_BUCKET = os.getenv("GCS_BUCKET", "kkbox-churn-prediction-493716-data")
GCS_MODEL_PREFIX = os.getenv("GCS_MODEL_PREFIX", "models/kkbox-churn-xgboost")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "kkbox-churn-prediction-493716")
CHURN_THRESHOLD = float(os.getenv("CHURN_THRESHOLD", "0.781"))

GENDER_MAP = {"male": 0, "female": 1, "unknown": 2}


def _load_artifacts() -> tuple[xgb.XGBClassifier, dict, list[str]]:
    """Download model + config from GCS and return (model, preprocessing_config, feature_cols)."""
    logger.info("Loading model artifacts from gs://%s/%s", GCS_BUCKET, GCS_MODEL_PREFIX)
    client = gcs.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)

    with tempfile.NamedTemporaryFile(suffix=".ubj", delete=False) as tmp:
        bucket.blob(f"{GCS_MODEL_PREFIX}/model.ubj").download_to_filename(tmp.name)
        model = xgb.XGBClassifier()
        model.load_model(tmp.name)

    preprocessing_config = json.loads(
        bucket.blob(f"{GCS_MODEL_PREFIX}/preprocessing_config.json").download_as_text()
    )
    feature_cols = json.loads(
        bucket.blob(f"{GCS_MODEL_PREFIX}/feature_cols.json").download_as_text()
    )["feature_cols"]

    logger.info("Model artifacts loaded. Features: %d", len(feature_cols))
    return model, preprocessing_config, feature_cols


def _preprocess(features: dict, config: dict, feature_cols: list[str]) -> pd.DataFrame:
    """Apply the same preprocessing as training before calling model.predict_proba."""
    row = {}
    for col in feature_cols:
        val = features.get(col)

        if col in config["num_cols_fill_zero"]:
            row[col] = val if val is not None else 0
        elif col == "bd":
            row[col] = val if val is not None else config["bd_median"]
        elif col == "city":
            row[col] = val if val is not None else config["city_mode"]
        elif col == "registered_via":
            row[col] = val if val is not None else config["registered_via_mode"]
        elif col == "gender":
            gender_str = val if val is not None else "unknown"
            row[col] = config["gender_map"].get(gender_str, 2)
        else:
            row[col] = val if val is not None else 0

    return pd.DataFrame([row], columns=feature_cols)


class PredictionService:
    def __init__(self):
        repo_path = os.getenv("FEAST_REPO_PATH")
        if not repo_path:
            repo_path = "/app/feature_store" if os.path.exists("/app/feature_store") else "../feature_store"

        self.store = FeatureStore(repo_path=repo_path)
        self.feature_refs = [
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

        self.model, self.preprocessing_config, self.feature_cols = _load_artifacts()

    def _get_features(self, msno: str) -> dict:
        try:
            with FeastFetchTimer():
                feature_vector = self.store.get_online_features(
                    features=self.feature_refs,
                    entity_rows=[{"msno": msno}],
                ).to_dict()
            return {k: v[0] for k, v in feature_vector.items()}
        except Exception:
            logger.exception("Failed to fetch online features from Feast for msno=%s", msno)
            return {}

    def _predict(self, features: dict) -> float:
        X = _preprocess(features, self.preprocessing_config, self.feature_cols)
        return float(self.model.predict_proba(X)[0, 1])

    def predict_single(self, msno: str) -> dict:
        features = self._get_features(msno)
        churn_prob = self._predict(features)
        return {
            "msno": msno,
            "churn_probability": churn_prob,
            "is_churn": int(churn_prob >= CHURN_THRESHOLD),
        }

    def predict_batch(self, msnos: list[str]) -> list[dict]:
        return [self.predict_single(msno) for msno in msnos]
