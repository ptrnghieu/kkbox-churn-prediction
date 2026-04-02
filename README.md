# Disease Trend Detection

An MLOps pipeline for detecting flu/disease outbreaks across US states.

The system ingests weekly ILI (Influenza-Like Illness) surveillance data from multiple
public health sources, engineers ML-ready features, and serves them through a feature
store for downstream outbreak prediction models.

**Core question:** Is a flu outbreak occurring or imminent in a given US state?

---

## Project Status

| Component | Status | Branch |
|---|---|---|
| Data pipeline (ingestion + feature store) | Done | `feature/kafka-ingestion` |
| Kafka streaming ingestion | In progress | `feature/kafka-ingestion` |
| Model pipeline (training + MLflow) | Not started | — |
| Serving pipeline (FastAPI + Gradio) | Not started | — |
| Airflow orchestration | Not started | — |
| Monitoring (Prometheus + Grafana + Evidently) | Not started | — |
| Kubernetes deployment | Not started | — |

---

## Repository Structure

```
disease-trend-detection/
├── data-pipeline/          # Data ingestion, transformation, feature store
│   ├── ingestion/          # Raw data fetchers (4 sources)
│   ├── transforms/         # Bronze → Silver → Gold Medallion transforms
│   └── feature_repo/       # Feast feature store definitions
│       └── DATA_CONTRACT.md  # Feature documentation for model team
├── infra/
│   └── docker/
│       ├── kafka/          # 3-broker KRaft Kafka cluster
│       └── minio/          # MinIO object storage
├── model_pipeline/         # (not yet built)
├── serving_pipeline/       # (not yet built)
└── infra/k8s/              # (not yet built)
```

---

## Quick Start

### Prerequisites
- Python 3.12+
- Docker + Docker Compose
- Git + DVC

### 1. Clone and set up infrastructure
```bash
git clone https://github.com/ptrnghieu/disease-trend-detection.git
cd disease-trend-detection

# Create shared Docker network
docker network create disease-trend-network

# Start MinIO (object storage) and Redis (online feature store)
cd infra/docker/minio && docker compose up -d
docker run -d --name redis -p 6379:6379 redis:7
```

### 2. Set up the data pipeline
```bash
cd data-pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Pull data from DVC
```bash
# Configure DVC to point to MinIO
dvc remote modify minio endpointurl http://localhost:9000

# Pull all data files
dvc pull
```

### 4. Set up the feature store
```bash
cd feature_repo/
feast apply
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")
```

That's it — the feature store is ready. See [data-pipeline/README.md](data-pipeline/README.md)
for full details, or [data-pipeline/feature_repo/DATA_CONTRACT.md](data-pipeline/feature_repo/DATA_CONTRACT.md)
for the feature documentation.

---

## Data Sources

| Source | Provider | Granularity | Coverage |
|---|---|---|---|
| ILINet (flu surveillance) | CDC via Delphi Epidata API | Weekly, state | 2010 → present |
| COVIDcast signals | CMU Delphi / CHNG / Facebook | Daily, state | 2020 → present |
| FluSurv-NET (hospitalizations) | CDC via Delphi Epidata API | Weekly, state | 2010 → present |
| Weather | Open-Meteo | Daily, state capital | 2018 → present |

---

## Technologies

| Layer | Tool |
|---|---|
| Data ingestion | Python, Requests |
| Data processing | Pandas, PyArrow |
| Feature store | Feast (offline: Parquet, online: Redis) |
| Data versioning | DVC + MinIO |
| Object storage | MinIO (S3-compatible) |
| Streaming | Apache Kafka (KRaft, 3-broker) |
| Containerization | Docker Compose |

---

## Team Contacts

| Team | Responsible for | Reference doc |
|---|---|---|
| Data pipeline | Ingestion, transforms, feature store | [data-pipeline/README.md](data-pipeline/README.md) |
| Model training | Training, evaluation, MLflow | [DATA_CONTRACT.md](data-pipeline/feature_repo/DATA_CONTRACT.md) |
| Serving | FastAPI, Gradio UI | `serving_pipeline/` (not yet built) |
| Infrastructure | Docker, Kubernetes | [infra/docker/README.md](infra/docker/README.md) |
