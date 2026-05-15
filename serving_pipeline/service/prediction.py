import logging
import os
import random

from app.metrics import FeastFetchTimer
from feast import FeatureStore

logger = logging.getLogger(__name__)

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

    def _get_features(self, msno: str) -> dict:
        """Get features from Feast online store, fallback to default if not found"""
        try:
            with FeastFetchTimer():
                feature_vector = self.store.get_online_features(
                    features=self.feature_refs,
                    entity_rows=[{"msno": msno}]
                ).to_dict()
            
            return {k: v[0] for k, v in feature_vector.items()}
        except Exception:
            logger.exception("Failed to fetch online features from Feast")
            return {"is_auto_renew": 1, "total_secs_last_7_days": 0}

    def _mock_model_predict(self, msno: str, features: dict) -> float:
        """Fake model"""
        return random.random()
    
    def predict_single(self, msno: str) -> dict:
        """Single prediction"""
        features = self._get_features(msno)
        churn_prob = self._mock_model_predict(msno, features)
        
        is_churn = 1 if churn_prob > 0.5 else 0
        
        return {
            "msno": msno,
            "churn_probability": churn_prob,
            "is_churn": is_churn
        }

    def predict_batch(self, msnos: list[str]) -> list[dict]:
        """Batch prediction"""
        return [self.predict_single(msno) for msno in msnos]
