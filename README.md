# KKBox Churn Prediction

End-to-end MLOps pipeline dự đoán churn của người dùng KKBox music streaming, triển khai trên GCP.

Dự án sử dụng chiến lược **historical playback**: dữ liệu 2017 (Bronze `_v2` files) được replay qua Kafka để giả lập môi trường streaming thực tế, phục vụ online prediction và monitoring liên tục.

## Architecture

```
GCS Bronze (_v2 CSVs)
    │
    ▼ kafka_producer.py  (streams trực tiếp từ GCS, không ghi disk)
Kafka  (kkbox.user_logs, kkbox.transactions)
    │  EOD marker trên cả hai topics sau mỗi ngày
    ▼ kafka_consumer.py  (spawned bởi FastAPI /stream/start)
BigQuery Gold  (features_streaming, append-only, partitioned by event_timestamp)
    │
    ▼ feast materialize  (background thread, chạy sau mỗi ngày)
Redis (Cloud Memorystore)  -- latest cumulative features per msno
    │
    ▼
FastAPI (:8000)  <--  nginx (:80)  <--  users
    │
    ▼
React Dashboard (/ui/)
  - Single User Predict
  - Batch Predict
  - Statistics
  - Streaming Simulation
  - Model Info
  - API Health
```

## Data Strategy

| Layer | Nội dung | Trạng thái |
|-------|----------|-----------|
| Bronze | Raw CSV gốc từ Kaggle (toàn bộ thời gian) | Đã load lên GCS |
| Silver | Cleaned & deduplicated Parquet (Spark) | Đã xử lý |
| Gold / features_train | Feature snapshot đến 2016-12-31, dùng cho training | Đang dùng |
| Gold / features_streaming | Streaming updates từ 2017, append-only | Ghi bởi consumer |

## Modules

| Module | Mô tả |
|--------|-------|
| `data_pipeline/` | Kafka producer/consumer, Spark Bronze→Silver→Gold |
| `model_pipeline/` | XGBoost training, MLflow tracking, model upload lên GCS |
| `feature_store/` | Feast definitions (entity, feature views), feast.yaml |
| `serving_pipeline/` | FastAPI online/batch prediction, React dashboard |
| `monitoring_pipeline/` | Prometheus scraping, Grafana dashboards |

## Deployment

- **VM:** `kkbox-serving` — GCE e2-standard-2, asia-southeast1-b
- **Static IP:** `35.198.232.66`
- **Dashboard:** http://35.198.232.66/ui/
- **nginx:** reverse proxy port 80 → uvicorn port 8000
- **systemd:** service `kkbox-serving`, auto-restart on crash/reboot
- **Kafka:** Docker (confluentinc/cp-kafka:7.5.0), ports 9092 (internal) / 9093 (host)
- **Redis:** Cloud Memorystore tại `10.80.68.19:6379`
- **GCS Bucket:** `gs://kkbox-churn-prediction-493716-data/`
- **BigQuery:** project `kkbox-churn-prediction-493716`

## Model Results

| Metric | Value |
|--------|-------|
| AUC-ROC | 0.8924 |
| AUC-PR | 0.5044 |
| F1 | 0.5068 |
| Precision | 0.3593 |
| Recall | 0.8596 |
| Optimal threshold | 0.789 |

Split strategy: **out-of-time** theo `registration_init_time < 2016-06-01`.
Train: ~804k rows — Test: ~157k rows — Churn rate: ~10%.

## Quick Start (local dev)

```bash
git clone <repo-url>
cd kkbox-churn-prediction

# Start Kafka (KRaft mode, no ZooKeeper)
docker compose up -d kafka

# Apply Feast definitions
feast -c feature_store apply

# Train model (reads from BigQuery)
python model_pipeline/training/train.py

# Start API server
cd serving_pipeline
uvicorn app.main:app --reload --port 8000
```

Truy cập dashboard tại http://localhost:8000/ui/

## GCP Resources

| Resource | Value |
|----------|-------|
| Project ID | `kkbox-churn-prediction-493716` |
| GCS Bucket | `gs://kkbox-churn-prediction-493716-data/` |
| Model path | `gs://kkbox-churn-prediction-493716-data/models/kkbox-churn-xgboost/` |
| BQ training table | `kkbox-churn-prediction-493716.kkbox_gold.features_train` |
| BQ streaming table | `kkbox-churn-prediction-493716.kkbox_gold.features_streaming` |
| Redis | `10.80.68.19:6379` (Cloud Memorystore) |

## Repository Layout

```
kkbox-churn-prediction/
├── docker-compose.yml          # Kafka, Redis (local dev), MLflow, api_serving
├── feature_store/              # Feast definitions
│   ├── feature_store.yaml
│   ├── entities.py
│   └── feature_views.py
├── data_pipeline/
│   ├── ingestion/
│   │   ├── kafka_producer.py   # Streams _v2 CSVs từ GCS → Kafka
│   │   └── kafka_consumer.py   # Kafka → BigQuery + Feast Redis
│   └── processing/
│       ├── bronze_to_silver.py # Spark: clean, cast, deduplicate
│       └── silver_to_gold.py   # Spark: feature aggregation, join
├── model_pipeline/
│   └── training/
│       ├── train.py
│       └── update_preprocessing_config.py
├── serving_pipeline/
│   ├── app/
│   │   ├── main.py
│   │   ├── predict.py
│   │   ├── explain.py
│   │   ├── stream.py
│   │   ├── feature_cache.py
│   │   ├── metrics.py
│   │   ├── schemas.py
│   │   └── stats_store.py
│   ├── service/
│   │   └── prediction.py
│   └── static/                 # React dashboard (CDN + Babel, no build step)
│       ├── index.html
│       ├── pages.jsx
│       └── charts.jsx
└── monitoring_pipeline/
    ├── prometheus.yml
    ├── docker-compose.yml
    └── grafana/
        ├── provisioning/
        └── dashboards/
```

## Environment Variables

Các biến quan trọng cần thiết lập (có thể dùng file `.env`):

| Variable | Default | Mô tả |
|----------|---------|-------|
| `GCP_PROJECT_ID` | `kkbox-churn-prediction-493716` | GCP project |
| `GCS_BUCKET` | `kkbox-churn-prediction-493716-data` | GCS bucket chứa model |
| `FEAST_REPO_PATH` | `../feature_store` | Đường dẫn tới Feast repo |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `CHURN_THRESHOLD` | `0.781` | Ngưỡng quyết định churn |
