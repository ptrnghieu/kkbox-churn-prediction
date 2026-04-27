# KKBox Churn Prediction

End-to-end MLOps pipeline dự đoán churn của KKBox music streaming trên GCP.

## Architecture

```
Kaggle → Kafka (simulated) → GCS Bronze → Spark → Silver → Gold
                                                              ↓
                                                    Feast Feature Store
                                                    ├── Offline (BigQuery)
                                                    └── Online (Redis)
                                                              ↓
                                               XGBoost + MLflow Training
                                                              ↓
                                                    FastAPI Serving
                                                              ↓
                                             Prometheus + Grafana + Streamlit
```

## Modules

| Module                 | Owner | Status         |
| ---------------------- | ----- | -------------- |
| `data_pipeline/`       | TBD   | 🟡 In Progress |
| `model_pipeline/`      | TBD   | 🔴 TODO        |
| `serving_pipeline/`    | TBD   | 🔴 TODO        |
| `monitoring_pipeline/` | TBD   | 🔴 TODO        |

## Quick Start

```bash
# Clone và setup
git clone <repo-url>
cd kkbox-churn-prediction
make setup

# Start services
make start

# Chạy tests
make test
```

## GCP Resources

- Project: `kkbox-churn-prediction-493716`
- GCS Bucket: `gs://kkbox-churn-prediction-493716-data/`
- BigQuery: `kkbox_gold.features` (1,082,190 rows)

## Feature Store

- Offline Store: BigQuery
- Online Store: Redis
- How to set up:
  cd serving_pipeline
  feast -c ../feature_store apply

## API serving

- How to start:
  cd serving_pipeline
  uvicorn app.main:app --reload

## Docs
