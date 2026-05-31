# Serving Pipeline

FastAPI serving layer cho KKBox churn prediction: online/batch prediction, SHAP explanations, React dashboard, và streaming simulation controller.

## Responsibilities

- Online prediction từ Redis feature store (single user + batch)
- SHAP feature explanation qua TreeExplainer
- React dashboard với 6 pages
- Streaming simulation: spawn/manage Kafka producer+consumer subprocesses
- SSE (Server-Sent Events) cho real-time UI updates

## File Structure

```
serving_pipeline/
├── app/
│   ├── main.py            -- FastAPI app, router registration
│   ├── stream.py          -- Streaming simulation controller
│   ├── feature_cache.py   -- In-memory msno → last streaming date
│   ├── metrics.py         -- Prometheus metrics
│   ├── explain.py         -- /explain endpoint
│   └── schemas.py         -- Pydantic request/response schemas
├── service/
│   └── prediction.py      -- PredictionService: load model, predict, SHAP
├── static/
│   ├── index.html         -- React app shell + CSS
│   ├── pages.jsx          -- Page components (CDN React + Babel)
│   ├── charts.jsx         -- Chart components
│   └── tweaks-panel.jsx   -- Settings panel
└── requirements.txt
```

## Deployment (production)

App chạy trên GCE VM `kkbox-serving` (asia-southeast1-b, static IP `35.198.232.66`):

- nginx reverse proxy: port 80 → uvicorn port 8000
- systemd service `kkbox-serving`: auto-restart on crash/reboot
- Dashboard: http://35.198.232.66/ui/

```bash
# Xem logs
journalctl -u kkbox-serving -f

# Restart
sudo systemctl restart kkbox-serving

# Status
sudo systemctl status kkbox-serving
```

## Run (local dev)

```bash
cd serving_pipeline
source ../venv/bin/activate   # hoặc venv của riêng serving_pipeline
uvicorn app.main:app --reload --port 8000
```

## API Endpoints

### Prediction

| Method | Path | Mô tả |
|--------|------|-------|
| POST | `/predict/` | Single user prediction |
| POST | `/predict/batch` | Batch prediction (CSV list) |
| POST | `/explain/` | SHAP feature explanation |
| GET | `/sample` | Random msno sample từ Redis |

**Single prediction request:**
```json
{ "msno": "<base64-encoded user id>" }
```

**Single prediction response:**
```json
{
  "msno": "...",
  "churn_probability": 0.84,
  "is_churn": 1,
  "member_found": true,
  "reasons": ["Khách hàng đã hủy gói nhiều lần", "..."],
  "feature_timestamp": "2017-03-05"
}
```

### Streaming Simulation

| Method | Path | Mô tả |
|--------|------|-------|
| POST | `/stream/start` | Spawn producer + consumer, bắt đầu replay |
| POST | `/stream/pause` | SIGSTOP producer |
| POST | `/stream/resume` | SIGCONT producer |
| POST | `/stream/stop` | SIGTERM producer + consumer, reset state |
| GET | `/stream/status` | Trạng thái hiện tại |
| GET | `/stream/users` | Danh sách msno theo ngày (paginated) |
| GET | `/stream/events` | SSE stream: `status` và `date_done` events |
| POST | `/stream/notify` | Internal: consumer gọi sau mỗi ngày flush |

### Infrastructure

| Method | Path | Mô tả |
|--------|------|-------|
| GET | `/health` | Health check |
| GET | `/metrics` | Prometheus scrape target |
| GET | `/stats` | Prediction statistics |
| GET | `/stats/churned` | Danh sách predicted churn users |

## Streaming Simulation

`/stream/start` spawn hai subprocess:

1. **Consumer** (trước): khởi động với `auto_offset_reset=latest`, lock Kafka offset
2. **Producer** (sau 20s): bắt đầu stream từ GCS

Mỗi ngày hoàn thành, consumer gọi `/stream/notify` → FastAPI broadcast SSE `date_done` event → UI hiện users mới.

Pause/Resume dùng SIGSTOP/SIGCONT trên producer process.

SSE initial event gửi `dates_list` đầy đủ để UI restore state khi reconnect hoặc switch tab.

## PredictionService

`service/prediction.py` load từ GCS khi khởi động:
- `model.ubj` — XGBoost model
- `preprocessing_config.json` — medians, modes cho imputation
- `feature_cols.json` — ordered feature list

Cold-start users (không có features trong Redis) được fill bằng **population median** từ training data (không dùng zeros).

SHAP TreeExplainer được khởi tạo một lần, reuse cho tất cả requests.

## Environment Variables

| Variable | Default | Mô tả |
|----------|---------|-------|
| `GCS_BUCKET` | `kkbox-churn-prediction-493716-data` | GCS bucket chứa model |
| `GCS_MODEL_PREFIX` | `models/kkbox-churn-xgboost` | Prefix của model artifacts |
| `GCP_PROJECT_ID` | `kkbox-churn-prediction-493716` | GCP project |
| `CHURN_THRESHOLD` | `0.781` | Ngưỡng phân loại churn |
| `FEAST_REPO_PATH` | `/app/feature_store` | Đường dẫn Feast repo |

## React Dashboard

Dashboard dùng React 18 + Babel standalone (CDN, không cần build step). 6 pages:

| Page | Mô tả |
|------|-------|
| Single User | Predict churn + SHAP explanation cho một msno |
| Batch Prediction | Upload CSV → score toàn bộ, download kết quả |
| Statistics | Cumulative prediction counts, churn rate |
| Streaming Simulation | Replay March 2017: date chips, user list, predict |
| Model Info | Metrics, ROC curve, feature importance |
| API Health | Service status, endpoint latency |

Tất cả pages được mount đồng thời (CSS `display:none/block`) để preserve state khi switch tab.
