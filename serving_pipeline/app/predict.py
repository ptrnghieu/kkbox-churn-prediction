from fastapi import APIRouter, Depends
from app.metrics import (
    BATCH_PREDICTION_SIZE,
    PREDICTION_REQUESTS_TOTAL,
    observe_prediction_result,
)
from app.schemas import BatchPredictRequest, PredictRequest, PredictResponse
from app import stats_store
from service.prediction import PredictionService

router = APIRouter()

def get_prediction_service():
    return PredictionService()

@router.post("/", response_model=PredictResponse, tags=["Prediction"])
def predict_churn(data: PredictRequest, service: PredictionService = Depends(get_prediction_service)):
    PREDICTION_REQUESTS_TOTAL.labels(endpoint="/predict/", kind="single").inc()
    result = service.predict_single(msno=data.msno)
    observe_prediction_result(
        endpoint="/predict/",
        churn_probability=result["churn_probability"],
        is_churn=result["is_churn"],
    )
    stats_store.record(result["is_churn"])
    return PredictResponse(**result)

@router.post("/batch", response_model=list[PredictResponse], tags=["Batch Prediction"])
def batch_predict_churn(
    data: BatchPredictRequest,
    service: PredictionService = Depends(get_prediction_service)
):
    PREDICTION_REQUESTS_TOTAL.labels(endpoint="/predict/batch", kind="batch").inc()
    BATCH_PREDICTION_SIZE.observe(len(data.msno_list))
    results = service.predict_batch(data.msno_list)
    for result in results:
        observe_prediction_result(
            endpoint="/predict/batch",
            churn_probability=result["churn_probability"],
            is_churn=result["is_churn"],
        )
        stats_store.record(result["is_churn"])
    return [PredictResponse(**res) for res in results]


