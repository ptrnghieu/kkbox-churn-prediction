"""FastAPI application entry point."""
import random
from time import perf_counter

import redis as redis_lib
from fastapi import FastAPI, HTTPException, Request, Response
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


@app.get("/sample", tags=["Utility"])
def sample_msnos(n: int = 5):
    """Return n random msno values that exist in the online feature store."""
    try:
        r = redis_lib.Redis(host="10.80.68.19", port=6379, socket_timeout=3)
        # RANDOMKEY is O(1) — call it n times
        msnos = set()
        attempts = 0
        while len(msnos) < n and attempts < n * 10:
            raw = r.randomkey()
            attempts += 1
            if raw is None:
                break
            s = raw.decode("latin-1")
            if "msno" in s and "kkbox_churn" in s:
                try:
                    start = s.index("msno") + 9  # 4 (tag) + 5 (length prefix bytes)
                    end = s.index("kkbox_churn")
                    msno = s[start:end]
                    if len(msno) > 10:
                        msnos.add(msno)
                except ValueError:
                    pass
        return {"msnos": list(msnos)}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Cannot reach feature store: {exc}")

@app.get("/test_error", tags=["Testing"])
def test_error():
    raise HTTPException(status_code=500, detail="This is a test error")

@app.get("/metrics", tags=["Monitoring"], include_in_schema=False)
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


app.include_router(predict_router, prefix="/predict")
