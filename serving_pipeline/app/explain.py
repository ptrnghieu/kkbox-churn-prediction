"""SHAP-based prediction explanation endpoint — returns raw feature contributions."""
import logging

from fastapi import APIRouter, Depends

from app.schemas import PredictRequest
from app.predict import get_prediction_service
from service.prediction import PredictionService, _preprocess

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Explanation"])


@router.post("/")
def explain_prediction(
    data: PredictRequest,
    service: PredictionService = Depends(get_prediction_service),
):
    """Return SHAP feature contributions for a single member prediction.

    Response:
      {
        "msno": "...",
        "shap_values": { feature: float, ... },  # sorted by |value| descending
        "base_value": float                       # model expected output
      }

    Positive shap_value → increases churn risk.
    Negative shap_value → decreases churn risk.
    """
    features = service._get_features(data.msno)
    X = _preprocess(features, service.preprocessing_config, service.feature_cols)

    shap_vals = service.explainer.shap_values(X)[0]  # shape: (n_features,)
    base_value = float(service.explainer.expected_value)

    raw = dict(zip(service.feature_cols, shap_vals.tolist()))
    sorted_shap = dict(
        sorted(raw.items(), key=lambda kv: abs(kv[1]), reverse=True)
    )

    return {
        "msno": data.msno,
        "shap_values": sorted_shap,
        "base_value": base_value,
    }
