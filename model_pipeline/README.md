# Model Pipeline

## Responsibilities
- Feast feature store definitions
- XGBoost training với out-of-time validation
- MLflow experiment tracking và model registry

## Files
- `feature_store/feature_store.yaml` — Feast config
- `feature_store/entities.py` — Entity definitions
- `feature_store/feature_views.py` — Feature view definitions
- `training/train.py` — Training script
- `training/evaluate.py` — Evaluation và metrics

## Run
```bash
# Materialize features vào Redis
feast -c model_pipeline/feature_store materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%S)

# Train model
python model_pipeline/training/train.py
```
