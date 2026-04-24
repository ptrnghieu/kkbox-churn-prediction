"""FastAPI application entry point."""
from fastapi import FastAPI
from app.predict import router as predict_router

app = FastAPI(
    title="KKBox Churn Prediction API",
    version="1.0.0",
)

@app.get("/health", tags=["Health Check"])
def health_check():
    return {"status": "healthy"}

app.include_router(predict_router, prefix="/predict")
