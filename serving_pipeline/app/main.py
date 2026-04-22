"""FastAPI application entry point."""
from fastapi import FastAPI
from serving_pipeline.app.routers import predict, health

app = FastAPI(
    title="KKBox Churn Prediction API",
    version="1.0.0",
)

app.include_router(health.router)
app.include_router(predict.router, prefix="/predict")
