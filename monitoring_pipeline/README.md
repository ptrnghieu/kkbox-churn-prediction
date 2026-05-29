# Monitoring Pipeline

Prometheus + Grafana for infrastructure metrics, Streamlit for business dashboard.

---

## Architecture

```
FastAPI (:8000/metrics)
        │
        ▼ scrape every 15s
   Prometheus (:9090)
        │
        ▼ datasource
    Grafana (:3000)          Streamlit (:8501)
                                    │
                                    ▼ HTTP POST
                             FastAPI (:8000/predict)
```

All services run on the same Docker network `churn-network` created by the root `docker-compose.yml`.

---

## Prerequisites

- Docker + Docker Compose installed on the deployment VM
- Root `docker-compose.yml` already running (FastAPI `api_serving` container must be up)
- No extra GCP credentials needed — this stack is purely internal

---

## Quick Start

```bash
# From repo root — start the core stack first (FastAPI must be running)
docker compose up -d

# Then start monitoring stack
cd monitoring_pipeline
docker compose up -d

# Verify all containers are up
docker compose ps
```

| Service | URL | Default credentials |
|---|---|---|
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Streamlit | http://localhost:8501 | — |

---

## Prometheus

### Scrape target

`prometheus.yml` scrapes `http://api_serving:8000/metrics` every 15s.  
The target name `api_serving` resolves via Docker's internal DNS on `churn-network`.

To verify scraping is working:
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

---

## Grafana Dashboards

Grafana auto-provisions the Prometheus datasource on startup via `grafana/provisioning/datasources/prometheus.yml`. No manual setup needed.

### Dashboard 1 — API Health

Build this dashboard at http://localhost:3000 with the panels below.

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

---

## Streamlit Dashboard

Business-facing dashboard for non-technical users.

**Features:**
- **Single prediction**: Enter an `msno` → get churn probability + label
- **Batch prediction**: Upload CSV with `msno` column → download results CSV + see score distribution chart

**Environment variable:**

| Variable | Default | Description |
|---|---|---|
| `FASTAPI_URL` | `http://localhost:8000` | FastAPI base URL |

Inside Docker the value should be `http://api_serving:8000` (set in `docker-compose.yml`).

---

## Deployment on GCE VM

The monitoring stack runs on the same VM as the core stack (or a separate VM in the same VPC).

### Same VM (recommended for single-node)

```bash
# Core stack already running
docker compose up -d          # from repo root

# Start monitoring
cd monitoring_pipeline
docker compose up -d
```

### Separate VM

If running on a dedicated monitoring VM, the `api_serving` container won't be reachable by name.  
Update `prometheus.yml` to use the core VM's internal IP:

```yaml
static_configs:
  - targets: ["<CORE_VM_INTERNAL_IP>:8000"]
```

Also set `FASTAPI_URL=http://<CORE_VM_INTERNAL_IP>:8000` in the Streamlit service environment.

### Firewall rules (GCP)

Open these ports on the VM's firewall tag if accessing from outside the VPC:

| Port | Service | Recommended access |
|---|---|---|
| 3000 | Grafana | Your IP only (IAP tunnel preferred) |
| 8501 | Streamlit | Your IP only |
| 9090 | Prometheus | Internal VPC only |

```bash
# Example: allow Grafana from your IP
gcloud compute firewall-rules create allow-grafana \
  --allow tcp:3000 \
  --source-ranges <YOUR_IP>/32 \
  --target-tags monitoring-vm
```

Using IAP tunnel (no firewall rule needed):
```bash
gcloud compute ssh <VM_NAME> --zone=<ZONE> -- -L 3000:localhost:3000 -L 8501:localhost:8501
```
Then open http://localhost:3000 and http://localhost:8501 locally.

---

## Checklist

- [ ] Core stack running (`docker compose ps` shows `api_serving` Up)
- [ ] `docker compose up -d` in `monitoring_pipeline/` succeeds
- [ ] Prometheus target `kkbox_serving_api` shows UP at http://localhost:9090/targets
- [ ] Grafana datasource Prometheus shows green at http://localhost:3000/datasources
- [ ] Dashboard 1 (API Health) created and panels returning data
- [ ] Dashboard 2 (Churn Prediction) created and panels returning data
- [ ] Streamlit single prediction working
- [ ] Streamlit batch prediction working
- [ ] Dashboard JSONs committed to `grafana/dashboards/`
