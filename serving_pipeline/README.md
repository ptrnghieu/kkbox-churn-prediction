# Serving Pipeline

## Responsibilities
- FastAPI REST API cho online và batch prediction
- Online features từ Feast Redis store

## Endpoints
- POST /predict/online — single user prediction
- POST /predict/batch — batch prediction
- GET /health — health check

## Run
```bash
uvicorn serving_pipeline.app.main:app --reload --port 8000
```
