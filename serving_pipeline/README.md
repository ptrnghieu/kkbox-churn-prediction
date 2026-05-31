# Serving Pipeline

FastAPI REST API cho online và batch churn prediction, kết hợp React dashboard phục vụ streaming simulation và monitoring.

Truy cập live tại: **http://35.198.232.66/ui/**

## Responsibilities

- Online prediction (single user) và batch prediction
- SHAP-based explainability cho từng prediction
- Streaming simulation: spawn/manage Kafka producer+consumer subprocesses
- React dashboard với 6 trang: Single User, Batch, Statistics, Streaming Simulation, Model Info, API Health
- Expose Prometheus metrics tại `/metrics`

## File Structure

```
serving_pipeline/
├── app/
│   ├── main.py            -- FastAPI app, middleware Prometheus, static mount /ui
│   ├── predict.py         -- POST /predict/, /predict/batch, /predict/explain
│   ├── explain.py         -- POST /explain/ router
│   ├── stream.py          -- POST /stream/start|pause|resume|stop, GET /stream/status|users|events
│   ├── feature_cache.py   -- In-memory msno → last streaming date mapping
│   ├── metrics.py         -- Prometheus metric definitions
│   ├── schemas.py         -- Pydantic request/response schemas
│   └── stats_store.py     -- In-memory prediction statistics store
├── service/
│   └── prediction.py      -- PredictionService: Feast → preprocess → XGBoost → SHAP
├── static/                -- React dashboard (CDN + Babel, không cần build step)
│   ├── index.html
│   ├── pages.jsx          -- 6 trang dashboard
│   ├── charts.jsx         -- Chart components
│   └── tweaks-panel.jsx   -- Settings panel
├── Dockerfile
└── requirements.txt
```

## Deployment (production)

App chạy trên GCE VM `kkbox-serving` (asia-southeast1-b, static IP `35.198.232.66`):

- nginx reverse proxy: port 80 → uvicorn port 8000
- systemd service `kkbox-serving`: auto-restart on crash/reboot
- Dashboard: http://35.198.232.66/ui/

```bash
# Xem logs
sudo journalctl -u kkbox-serving -f

# Restart
sudo systemctl restart kkbox-serving

# Status
sudo systemctl status kkbox-serving
```

## Run (local dev)

```bash
cd serving_pipeline
pip install -r requirements.txt

# Cần set FEAST_REPO_PATH trỏ tới feature_store/
export FEAST_REPO_PATH=../feature_store

uvicorn app.main:app --reload --port 8000
```

Dashboard: http://localhost:8000/ui/
API docs: http://localhost:8000/docs

## API Endpoints

### Prediction

| Method | Path | Mô tả |
|--------|------|-------|
| POST | `/predict/` | Single user prediction với SHAP reasons |
| POST | `/predict/batch` | Batch prediction (list msno), không SHAP |
| POST | `/predict/explain` | Alias cho `/predict/` — luôn trả về SHAP reasons |
| POST | `/explain/` | SHAP explanation endpoint |
| GET | `/sample` | Random msno từ Redis (dùng để demo) |

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
  "reasons": ["Khách hàng đã hủy gói nhiều lần", "Ít bài hát được nghe hoàn toàn"],
  "feature_timestamp": "2017-03-05"
}
```

**Batch prediction request:**
```json
{ "msno_list": ["abc==", "def==", "ghi=="] }
```

### Streaming Simulation

| Method | Path | Mô tả |
|--------|------|-------|
| POST | `/stream/start` | Spawn producer + consumer, bắt đầu replay |
| POST | `/stream/pause` | SIGSTOP producer |
| POST | `/stream/resume` | SIGCONT producer |
| POST | `/stream/stop` | SIGTERM producer + consumer, reset state |
| GET | `/stream/status` | Trạng thái hiện tại: status, current_date, dates_done |
| GET | `/stream/users` | Danh sách msno theo ngày (paginated) |
| GET | `/stream/events` | SSE stream: `status` và `date_done` events |
| POST | `/stream/notify` | Internal: consumer gọi sau mỗi ngày flush |

**SSE events:**
- `status`: broadcast khi trạng thái thay đổi (start/pause/resume/stop/done)
- `date_done`: broadcast sau mỗi ngày được flush, kèm `date`, `user_count`, `dates_done`

### Infrastructure

| Method | Path | Mô tả |
|--------|------|-------|
| GET | `/health` | Health check: `{"status": "healthy"}` |
| GET | `/metrics` | Prometheus scrape target (text format) |
| GET | `/stats` | Cumulative prediction statistics từ lúc server start |
| GET | `/stats/churned` | Danh sách users dự đoán churn, sorted by probability |

## Streaming Simulation

`/stream/start` spawn hai subprocess theo thứ tự:

1. **Consumer** (trước): khởi động với `auto_offset_reset=latest`, lock Kafka offset tại thời điểm hiện tại
2. **Chờ 20 giây**: đảm bảo consumer đã join Kafka group trước khi producer bắt đầu gửi dữ liệu
3. **Producer** (sau 20s): bắt đầu stream từ GCS

Mỗi ngày hoàn thành:
- Consumer gọi `POST /stream/notify` → FastAPI cập nhật `current_date`, `dates_done`
- FastAPI broadcast SSE `date_done` event → Dashboard hiện date chip mới + danh sách users
- Background thread: BQ write + Feast materialize (không block consumer)

Pause/Resume dùng `SIGSTOP`/`SIGCONT` trên producer process (consumer tiếp tục chạy).

SSE initial event (khi browser kết nối) gửi `dates_list` đầy đủ — UI restore state khi reconnect hoặc refresh tab.

Streaming simulation cover **March 2017** (31 ngày). Speed mặc định: 55 giây/ngày.

## PredictionService (`service/prediction.py`)

**Startup (lazy-loaded singleton):**
1. Khởi tạo `FeatureStore` (Feast, trỏ tới Redis Cloud Memorystore `10.80.68.19:6379`)
2. Load model artifacts từ GCS: `model.ubj`, `preprocessing_config.json`, `feature_cols.json`
3. Khởi tạo `shap.TreeExplainer` (khởi tạo một lần, reuse cho tất cả requests)

**Per-request flow:**
1. Fetch 18 features từ Redis qua Feast `get_online_features`
2. Preprocess: cold-start users (không có features) fill bằng population median
3. `predict_proba` → churn probability
4. So sánh với `CHURN_THRESHOLD` → is_churn (0 hoặc 1)
5. SHAP → top 3 features có positive SHAP value → map sang human-readable reasons (tiếng Việt)

**Cold-start imputation:** user không có features trong Redis được fill bằng median từ `preprocessing_config.json`, không dùng zeros. Đảm bảo model thấy "average user" thay vì user hoàn toàn inactive.

**Batch prediction:** dùng `_predict_single_fast()` — bỏ qua SHAP computation để tăng tốc xử lý.

## React Dashboard

Dashboard dùng React 18 + Babel standalone qua CDN — không cần build step, không cần Node.js.

| Page | Mô tả |
|------|-------|
| Single User | Nhập msno → churn probability, SHAP reasons, "Features as of YYYY-MM-DD" |
| Batch Prediction | Nhập list msno → bảng kết quả |
| Statistics | Tổng số predictions, churn rate, danh sách churned users |
| Streaming Simulation | Start/pause/resume/stop, progress bar, date chips, paginated user list |
| Model Info | AUC-ROC, threshold, feature list, split strategy |
| API Health | Status các services, endpoint response |

Tất cả 6 pages mount đồng thời (`display: none/block`) — preserve state (đặc biệt SSE connection) khi switch tab.

## Prometheus Metrics

| Metric | Type | Labels |
|--------|------|--------|
| `serving_http_requests_total` | Counter | method, path, status_code |
| `serving_http_request_duration_seconds` | Histogram | method, path |
| `serving_http_requests_in_progress` | Gauge | method, path |
| `serving_prediction_requests_total` | Counter | endpoint, kind |
| `serving_prediction_results_total` | Counter | endpoint, is_churn |
| `serving_prediction_churn_probability` | Histogram | endpoint |
| `serving_batch_prediction_size` | Histogram | — |
| `serving_feast_online_fetch_total` | Counter | status |
| `serving_feast_online_fetch_duration_seconds` | Histogram | status |

## Environment Variables

| Variable | Default | Mô tả |
|----------|---------|-------|
| `GCS_BUCKET` | `kkbox-churn-prediction-493716-data` | GCS bucket chứa model |
| `GCS_MODEL_PREFIX` | `models/kkbox-churn-xgboost` | Prefix của model artifacts |
| `GCP_PROJECT_ID` | `kkbox-churn-prediction-493716` | GCP project |
| `CHURN_THRESHOLD` | `0.781` | Ngưỡng phân loại churn |
| `FEAST_REPO_PATH` | `/app/feature_store` | Đường dẫn Feast repo |

## Dependencies

| Package | Mục đích |
|---------|---------|
| `fastapi` | Web framework |
| `uvicorn` | ASGI server |
| `xgboost` | Model inference |
| `feast` | Online feature lookup từ Redis |
| `shap` | SHAP TreeExplainer |
| `prometheus-client` | Metrics exposition |
| `redis` | Direct Redis access (cho /sample endpoint) |
| `google-cloud-storage` | Load model artifacts từ GCS |
