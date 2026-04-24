from fastapi import APIRouter, Depends
from app.schemas import PredictRequest, PredictResponse
from service.prediction import PredictionService

router = APIRouter()

def get_prediction_service():
    return PredictionService()

@router.post("/", response_model=PredictResponse, tags=["Prediction"])
def predict_churn(data: PredictRequest, service: PredictionService = Depends(get_prediction_service)):
    result = service.predict_single(msno=data.msno)
    return PredictResponse(**result)

@router.post("/batch", response_model=list[PredictResponse], tags=["Batch Prediction"])
def batch_predict_churn(
    data: list[PredictRequest], 
    service: PredictionService = Depends(get_prediction_service)
):
    msnos = [item.msno for item in data]
    results = service.predict_batch(msnos)
    return [PredictResponse(**res) for res in results]