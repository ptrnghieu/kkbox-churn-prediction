from fastapi import APIRouter
from schemas import PredictRequest, PredictResponse

router = APIRouter()

@router.post("/predict", response_model=PredictResponse, tags=["Prediction"])
def predict_churn(data: dict):
    # Placeholder for actual prediction logic
    return {"prediction": "churn" if data.get("churn_risk", 0) > 0.5 else "no_churn"}