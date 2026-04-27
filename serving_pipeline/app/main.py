"""FastAPI application entry point."""
from time import perf_counter

from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.metrics import (
    HTTP_REQUEST_DURATION_SECONDS,
    HTTP_REQUESTS_IN_PROGRESS,
    HTTP_REQUESTS_TOTAL,
)
from app.predict import router as predict_router

app = FastAPI(
    title="KKBox Churn Prediction API",
    version="1.0.0",
)


@app.middleware("http")
async def prometheus_http_middleware(request: Request, call_next):
    method = request.method
    fallback_path = request.url.path
    started_at = perf_counter()
    in_progress_metric = HTTP_REQUESTS_IN_PROGRESS.labels(method=method, path=fallback_path)
    in_progress_metric.inc()

    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        route = request.scope.get("route")
        path = getattr(route, "path", fallback_path)
        elapsed = perf_counter() - started_at
        in_progress_metric.dec()

        HTTP_REQUESTS_TOTAL.labels(
            method=method,
            path=path,
            status_code=str(status_code),
        ).inc()
        HTTP_REQUEST_DURATION_SECONDS.labels(method=method, path=path).observe(elapsed)


@app.get("/health", tags=["Health Check"])
def health_check():
    return {"status": "healthy"}


@app.get("/metrics", tags=["Monitoring"], include_in_schema=False)
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


app.include_router(predict_router, prefix="/predict")
