# Model Pipeline

Training pipeline cho XGBoost churn prediction model: đọc features từ BigQuery Gold, train, track experiments qua MLflow, và upload model artifacts lên GCS.

## Responsibilities

- Train XGBoost model với out-of-time validation trên dữ liệu BigQuery Gold
- Track experiments và metrics bằng MLflow
- Upload model artifact, preprocessing config, và feature column list lên GCS
- Cung cấp script riêng để regenerate preprocessing config mà không cần retrain

## File Structure

```
model_pipeline/
└── training/
    ├── train.py                        -- Main training script
    └── update_preprocessing_config.py  -- Regenerate preprocessing_config.json từ BQ
```

Feature store definitions nằm ở `feature_store/` (root level — dùng chung cho cả training và serving):

```
feature_store/
├── feature_store.yaml   -- Feast config (GCP provider, BigQuery offline, Redis online)
├── entities.py          -- Entity: msno (user ID)
└── feature_views.py     -- Feature view: 18 features từ kkbox_gold.features_train
```

## Data

**Source:** `kkbox-churn-prediction-493716.kkbox_gold.features_train`
- ~1,082,190 rows, feature snapshot tính đến 2016-12-31
- Target column: `is_churn`
- Churn rate: ~10%

**Split strategy: out-of-time theo `registration_init_time`**

| Split | Điều kiện | Số rows |
|-------|-----------|---------|
| Train | `registration_init_time < 2016-06-01` | ~804k |
| Test  | `registration_init_time >= 2016-06-01` | ~157k |

Out-of-time split phản ánh thực tế production: model được train trên user đăng ký sớm hơn, test trên user mới hơn.

## Features (18 features)

| Feature | Nguồn | Mô tả |
|---------|-------|-------|
| `city` | members | Thành phố đăng ký |
| `bd` | members | Tuổi người dùng |
| `gender` | members | Giới tính (encoded) |
| `registered_via` | members | Kênh đăng ký |
| `total_transactions` | transactions | Tổng số giao dịch |
| `total_amount_paid` | transactions | Tổng tiền đã thanh toán |
| `avg_amount_paid` | transactions | Trung bình mỗi giao dịch |
| `auto_renew_count` | transactions | Số lần tự động gia hạn |
| `cancel_count` | transactions | Số lần hủy gói |
| `total_log_days` | user_logs | Số ngày có hoạt động |
| `total_secs` | user_logs | Tổng giây nghe nhạc |
| `avg_daily_secs` | user_logs | Trung bình giây/ngày |
| `total_num_25` | user_logs | Bài nghe được 25% |
| `total_num_50` | user_logs | Bài nghe được 50% |
| `total_num_75` | user_logs | Bài nghe được 75% |
| `total_num_985` | user_logs | Bài nghe được 98.5% |
| `total_num_100` | user_logs | Bài nghe hoàn toàn |
| `total_num_unq` | user_logs | Số bài hát unique |

## Model Results

| Metric | Value |
|--------|-------|
| AUC-ROC | **0.8924** |
| AUC-PR | 0.5044 |
| F1 | 0.5068 |
| Precision | 0.3593 |
| Recall | 0.8596 |
| Optimal threshold (F1-maximized) | **0.789** |

Threshold mặc định trong serving là `0.781` (có thể override qua env var `CHURN_THRESHOLD`).

## Run Training

```bash
# Mặc định: đọc từ BigQuery, log vào MLflow local, upload model lên GCS
python model_pipeline/training/train.py

# Tùy chỉnh hyperparameters
python model_pipeline/training/train.py \
  --n-estimators 1000 \
  --max-depth 5 \
  --learning-rate 0.03

# Xem tất cả options
python model_pipeline/training/train.py --help
```

Training đọc dữ liệu từ BigQuery, nên cần GCP credentials hợp lệ (`GOOGLE_APPLICATION_CREDENTIALS` hoặc ADC).

## GCS Model Artifacts

```
gs://kkbox-churn-prediction-493716-data/models/kkbox-churn-xgboost/
├── model.ubj                   -- XGBoost model (binary format)
├── preprocessing_config.json   -- Medians/modes cho cold-start imputation
└── feature_cols.json           -- Ordered list of 18 feature columns
```

`preprocessing_config.json` chứa:
- `num_cols_medians`: median của từng numeric feature (dùng cho cold-start)
- `num_cols_fill_zero`: list features được fill bằng median thay vì 0
- `bd_median`, `city_mode`, `registered_via_mode`: giá trị mặc định cho member features
- `gender_map`: mapping string → int cho gender

## Preprocessing Config

`preprocessing_config.json` phục vụ **cold-start imputation**: khi một user chưa có features trong Redis (user mới hoặc chưa xuất hiện trong streaming), serving pipeline điền bằng population median thay vì số 0 — giúp model thấy một "average user" thay vì user hoàn toàn inactive.

```bash
# Regenerate config mà không retrain model
# (dùng khi BQ data thay đổi nhưng không muốn retrain toàn bộ)
python model_pipeline/training/update_preprocessing_config.py
```

## Feast Feature Store

Feature store dùng chung cho cả training (offline) và serving (online).

```bash
# Apply definitions (tạo/update tables và online store schema)
feast -c feature_store apply

# Kiểm tra entities và feature views
feast -c feature_store entities list
feast -c feature_store feature-views list

# Materialize features từ BQ vào Redis (online store)
feast -c feature_store materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%S)
```

Trong production, materialize chạy tự động trong background thread của kafka_consumer.py sau mỗi ngày dữ liệu mới.

## MLflow Tracking

MLflow server chạy trong Docker (xem `docker-compose.yml`):
- Backend: SQLite (`mlflow.db`)
- Artifact store: `./artifacts`
- Port: 5000

```bash
# Start MLflow server (nếu chạy standalone, không dùng docker-compose)
mlflow server \
  --backend-store-uri sqlite:///mlflow.db \
  --default-artifact-root ./artifacts \
  --host 0.0.0.0 --port 5000

# Xem MLflow UI
open http://localhost:5000
```

## Environment Variables

| Variable | Default | Mô tả |
|----------|---------|-------|
| `GCP_PROJECT_ID` | `kkbox-churn-prediction-493716` | GCP project |
| `BQ_FEATURES_TABLE` | `kkbox-churn-prediction-493716.kkbox_gold.features_train` | BigQuery source |
| `GCS_MODEL_BUCKET` | `kkbox-churn-prediction-493716-data` | Bucket để upload model |
| `GCS_MODEL_PREFIX` | `models/kkbox-churn-xgboost` | Prefix trong bucket |
| `MLFLOW_TRACKING_URI` | `mlruns` | MLflow backend URI |
| `MLFLOW_EXPERIMENT_NAME` | `kkbox-churn-xgboost` | Tên experiment |

## Dependencies

| Package | Mục đích |
|---------|---------|
| `xgboost` | Model training |
| `google-cloud-bigquery` | Đọc training data |
| `google-cloud-storage` | Upload model artifacts |
| `mlflow` | Experiment tracking |
| `scikit-learn` | Preprocessing, metrics |
| `feast` | Feature store definitions |
| `shap` | SHAP explainability (dùng trong serving) |
