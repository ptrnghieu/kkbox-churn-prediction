# Model Pipeline

## Responsibilities
- Feast feature store definitions (entity, feature views)
- XGBoost training với out-of-time validation
- MLflow experiment tracking và model registry

## Files
- `feature_store/feature_store.yaml` — Feast config (GCP provider, BigQuery offline, Redis online)
- `feature_store/entities.py` — Entity: `msno` (user ID)
- `feature_store/feature_views.py` — Feature view: 18 features từ `kkbox_gold.features_train`
- `training/train.py` — Training script (BigQuery → preprocess → XGBoost → MLflow)

## Data

Source: `kkbox_gold.features_train` trên BigQuery — snapshot feature của ~1M users tính đến **2016-12-31**.

Split strategy: **out-of-time** theo `registration_init_time`:
- Train: `< 2016-06-01` (~804k rows)
- Test:  `>= 2016-06-01` (~157k rows)

## Run

```bash
# Apply Feast definitions
feast -c model_pipeline/feature_store apply

# Materialize features vào Redis (online store)
feast -c model_pipeline/feature_store materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%S)

# Train model
python model_pipeline/training/train.py

# Tùy chỉnh hyperparameters
python model_pipeline/training/train.py \
  --n-estimators 1000 \
  --max-depth 5 \
  --learning-rate 0.03
```

## Baseline Results

| Metric | Value |
|--------|-------|
| AUC-ROC | 0.8924 |
| AUC-PR | 0.5044 |
| F1 (t=0.5) | 0.5068 |
| Recall | 0.8596 |
| Best threshold | 0.789 |
