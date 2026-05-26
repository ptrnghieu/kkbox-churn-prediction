# KKBox Churn Prediction

End-to-end MLOps pipeline dự đoán churn của KKBox music streaming, triển khai cloud-native trên GCP.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  DATA INGESTION & TRANSFORMATION                                    │
│                                                                     │
│  Kaggle ──► Kafka ──► GCS Bronze ──► Spark ──► GCS Silver/Gold     │
│  (CSV)     (topics)   (raw CSV)    (clean,     (Parquet)            │
│                                    aggregate)                       │
│                                        │                            │
│                                        ▼                            │
│                              Feast Feature Store                    │
│                              ├── Offline: BigQuery                  │
│                              └── Online:  Redis                     │
└─────────────────────────────────────────────────────────────────────┘
                                        │
┌─────────────────────────────────────────────────────────────────────┐
│  MODEL TRAINING                                                     │
│                                                                     │
│  Training Features ──► XGBoost ──► MLflow ──► (New model?)         │
│  (BigQuery offline)    (train.py)   (tracking                       │
│                                     & registry)                     │
└─────────────────────────────────────────────────────────────────────┘
                                        │ Trigger (new model)
┌─────────────────────────────────────────────────────────────────────┐
│  SERVING                                                            │
│                                                                     │
│  Online Features (Redis) ──► FastAPI ◄── Roll out new model        │
│                                │                                    │
└────────────────────────────────┼────────────────────────────────────┘
                                 │
┌────────────────────────────────┼────────────────────────────────────┐
│  MONITORING                    ▼                                    │
│                                                                     │
│  Prometheus ◄── scrape ── FastAPI /metrics                          │
│      │                                                              │
│      └──► Grafana (visualization)                                   │
│                                                                     │
│  Streamlit (user-facing dashboard) ◄──► End user                   │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Strategy — Historical Playback

Dự án dùng chiến lược **historical playback** với dataset KKBox từ Kaggle:

| Layer | Nội dung | Trạng thái |
|-------|----------|-----------|
| **Bronze** | Raw CSV gốc từ Kaggle (toàn bộ thời gian) | ✅ Đã load lên GCS |
| **Silver** | Cleaned & deduplicated Parquet | ✅ Đã xử lý qua Spark |
| **Gold / features_train** | Feature-engineered, snapshot đến **2016-12-31** | ✅ Đang dùng để train |
| **Gold / phần còn lại** | Dữ liệu sau 2016-12-31, còn ở Bronze/Silver | 🔜 Dùng cho simulated streaming |

> Phần dữ liệu sau 2016-12-31 sẽ được replay qua Kafka để giả lập môi trường streaming thực tế, phục vụ online prediction và monitoring.

## Modules

| Module | Mô tả | Status |
|--------|-------|--------|
| `data_pipeline/` | Kafka ingestion, Spark Bronze→Silver→Gold | 🟡 In Progress |
| `model_pipeline/` | Feast definitions, XGBoost training, MLflow | 🟡 In Progress |
| `serving_pipeline/` | FastAPI online/batch prediction | 🟡 In Progress |
| `monitoring_pipeline/` | Prometheus, Grafana, Streamlit dashboard | 🔴 TODO |

## GCP Resources

- **Project:** `kkbox-churn-prediction-493716`
- **GCS Bucket:** `gs://kkbox-churn-prediction-493716-data/`
- **BigQuery table:** `kkbox_gold.features_train` — 1,082,190 rows, snapshot đến 2016-12-31

## Model — Baseline Results

| Metric | Giá trị |
|--------|---------|
| AUC-ROC | **0.8924** |
| AUC-PR | 0.5044 |
| F1 (threshold=0.5) | 0.5068 |
| Precision | 0.3593 |
| Recall | 0.8596 |
| Best threshold (F1-opt) | **0.789** |

Split strategy: **out-of-time** theo `registration_init_time < 2016-06-01`
Train: 804k rows · Test: 157k rows · Churn rate: ~10%

## Feature Store

- **Offline Store:** BigQuery (`kkbox_gold.features_train`)
- **Online Store:** Redis

```bash
cd serving_pipeline
source venv/bin/activate          # hoặc venv\Scripts\activate trên Windows
feast -c ../feature_store apply
```

## Quick Start

```bash
git clone <repo-url>
cd kkbox-churn-prediction
make setup
make start
make test
```

## Training

```bash
# Mặc định (đọc từ BigQuery, log vào MLflow local)
python model_pipeline/training/train.py

# Tùy chỉnh hyperparameters
python model_pipeline/training/train.py \
  --n-estimators 1000 \
  --max-depth 5 \
  --learning-rate 0.03
```

Environment variables:

<<<<<<< HEAD
- Check entities and features:
  feast -c ../feature_store entities list
  feast -c ../feature_store feature-views list

- Materialize features to online store:
  feast -c ../feature_store materialize 2026-01-1 2026-05-25

## API serving

- How to start:
  cd serving_pipeline
  venv\Scripts\activate or source venv/bin/activate
  uvicorn app.main:app --reload

  grafana: http://localhost:3000 (admin:admin)

# Mornitoring

- How to start:
  cd monitoring_pipeline
  docker-compose up -d

## Docs
=======
| Variable | Default | Mô tả |
|----------|---------|-------|
| `GCP_PROJECT_ID` | `kkbox-churn-prediction-493716` | GCP project |
| `BQ_FEATURES_TABLE` | `kkbox-churn-prediction-493716.kkbox_gold.features_train` | BigQuery source |
| `MLFLOW_TRACKING_URI` | `mlruns` | MLflow backend |
| `MLFLOW_EXPERIMENT_NAME` | `kkbox-churn-xgboost` | Tên experiment |
| `MODEL_NAME` | `kkbox-churn-model` | Tên registered model |

## Serving API

```bash
cd serving_pipeline
uvicorn app.main:app --reload --port 8000
```

| Endpoint | Method | Mô tả |
|----------|--------|-------|
| `/predict/` | POST | Single user prediction |
| `/predict/batch` | POST | Batch prediction |
| `/health` | GET | Health check |
| `/metrics` | GET | Prometheus metrics |

## Monitoring

```bash
cd monitoring_pipeline
docker-compose up -d
```

| Service | URL | Mô tả |
|---------|-----|-------|
| Prometheus | http://localhost:9090 | Metrics collection |
| Grafana | http://localhost:3000 | Dashboards (admin/admin) |
| Streamlit | http://localhost:8501 | User-facing dashboard |

## Spark Jobs

```bash
make spark-bronze-silver   # Bronze → Silver
make spark-silver-gold     # Silver → Gold
```
>>>>>>> 23a6bda890317f2f43ef6094774f0b93b58ed025
