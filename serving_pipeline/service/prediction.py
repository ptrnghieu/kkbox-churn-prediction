from feast import FeatureStore
import random
import logging

class PredictionService:
    def __init__(self):
        self.store = FeatureStore(repo_path="../feature_store")
        self.feature_refs = [
            "placeholder"
        ]

    def _get_features(self, msno: str) -> dict:
        """Get features from Feast online store, fallback to default if not found"""
        try:
            feature_vector = self.store.get_online_features(
                features=self.feature_refs,
                entity_rows=[{"msno": msno}]
            ).to_dict()
            
            return {k: v[0] for k, v in feature_vector.items()}
        except Exception as e:
            # TODO: Add logging here
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