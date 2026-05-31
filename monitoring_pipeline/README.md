# Monitoring Pipeline

Prometheus + Grafana for infrastructure and prediction metrics. Business dashboard is the React UI built into the FastAPI serving app (no Streamlit).

## Architecture

```
FastAPI (:8000/metrics)   ←  systemd service kkbox-serving on kkbox-serving VM
        │
        ▼ scrape every 15s
   Prometheus (:9090)     ←  Docker (monitoring_pipeline/docker-compose.yml)
        │
        ▼ datasource
    Grafana (:3000)       ←  Docker (monitoring_pipeline/docker-compose.yml)
```

Prometheus scrapes the VM's FastAPI `/metrics` endpoint via its internal IP or `localhost` if running on the same VM.

## Quick Start

```bash
# From monitoring_pipeline/
docker compose up -d

# Verify
docker compose ps
```

| Service | URL | Default credentials |
|---|---|---|
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |

FastAPI itself runs as a systemd service (not Docker). See `serving_pipeline/README.md` for deployment details.

To access Grafana from your local machine without opening firewall ports:
```bash
gcloud compute ssh kkbox-serving --zone=asia-southeast1-b -- -L 3000:localhost:3000
```
Then open http://localhost:3000.

## Prometheus

### Scrape target

`prometheus.yml` scrapes `http://localhost:8000/metrics` every 15s (same VM).

To verify scraping:
1. Open http://localhost:9090/targets
2. `kkbox_serving_api` should show **UP**

### Available metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `serving_http_requests_total` | Counter | method, path, status_code | Total HTTP requests |
| `serving_http_request_duration_seconds` | Histogram | method, path | Request latency (buckets: 5ms–10s) |
| `serving_http_requests_in_progress` | Gauge | method, path | In-flight requests |
| `serving_prediction_requests_total` | Counter | endpoint, kind | Prediction calls (single/batch) |
| `serving_prediction_results_total` | Counter | endpoint, is_churn | Churn vs retain counts |
| `serving_prediction_churn_probability` | Histogram | endpoint | Score distribution (buckets: 0.0–1.0) |
| `serving_batch_prediction_size` | Histogram | — | Records per batch call |
| `serving_feast_online_fetch_total` | Counter | status (success/error) | Feast Redis lookups |
| `serving_feast_online_fetch_duration_seconds` | Histogram | status | Feast lookup latency |

## Grafana Dashboards

Grafana auto-provisions the Prometheus datasource on startup via `grafana/provisioning/datasources/prometheus.yml`. No manual setup needed.

### Dashboard 1 — API Health

**Panel: Request Rate (RPS)**
```promql
rate(serving_http_requests_total[1m])
```

**Panel: P99 Latency**
```promql
histogram_quantile(0.99, rate(serving_http_request_duration_seconds_bucket[5m]))
```

**Panel: Error Rate**
```promql
rate(serving_http_requests_total{status_code=~"5.."}[1m])
/ rate(serving_http_requests_total[1m])
```

**Panel: In-Flight Requests**
```promql
serving_http_requests_in_progress
```

### Dashboard 2 — Churn Prediction

**Panel: Churn Rate (rolling 5m)**
```promql
rate(serving_prediction_results_total{is_churn="True"}[5m])
/ rate(serving_prediction_results_total[5m])
```

**Panel: Prediction Throughput**
```promql
rate(serving_prediction_requests_total[1m])
```

**Panel: Churn Probability Distribution**
```promql
rate(serving_prediction_churn_probability_bucket[5m])
```
Use as a heatmap visualization.

**Panel: Feast Fetch Success Rate**
```promql
rate(serving_feast_online_fetch_total{status="success"}[5m])
/ rate(serving_feast_online_fetch_total[5m])
```

**Panel: Feast P95 Latency**
```promql
histogram_quantile(0.95, rate(serving_feast_online_fetch_duration_seconds_bucket[5m]))
```

### Saving dashboards as JSON

After building dashboards in the UI:
1. Dashboard settings → JSON Model → Copy
2. Save to `monitoring_pipeline/grafana/dashboards/<name>.json`
3. Grafana auto-loads JSON files from that directory on restart

## File Structure

```
monitoring_pipeline/
├── docker-compose.yml          -- Prometheus + Grafana
├── prometheus.yml              -- Scrape config
└── grafana/
    ├── provisioning/
    │   └── datasources/
    │       └── prometheus.yml  -- Auto-provision Prometheus datasource
    └── dashboards/             -- Dashboard JSON files (committed here)
```

## Business Dashboard

The React dashboard at http://35.198.232.66/ui/ replaces the previous Streamlit dashboard. It is served directly by FastAPI and requires no separate deployment.

| Page | Description |
|------|-------------|
| Single User | Predict churn + SHAP explanation for one msno |
| Batch Prediction | Upload CSV → score all users, download results |
| Statistics | Cumulative prediction counts, churn rate |
| Streaming Simulation | Replay March 2017 data, date chips, user list, predict |
| Model Info | Metrics, ROC curve, feature importance |
| API Health | Service status, endpoint latency |
