# Data Pipeline

## Responsibilities
- Kafka historical playback streaming từ Bronze `_v2` files (2017 data)
- Spark Bronze→Silver→Gold transformation

## Files
- `ingestion/kafka_producer.py` — Replay Bronze `_v2` CSVs → Kafka topics theo thứ tự date
- `ingestion/kafka_consumer.py` — Consume Kafka → aggregate per day → BigQuery Gold → Feast materialize
- `processing/bronze_to_silver.py` — Spark: clean, cast, deduplicate
- `processing/silver_to_gold.py` — Spark: feature aggregation, join

## Data split (Bronze layer)

| File | Nội dung | Dùng cho |
|------|----------|----------|
| `user_logs.csv` (28.4 GB) | Pre-2017 logs | Training snapshot |
| `user_logs_v2.csv` (1.3 GB) | 2017 logs | **Streaming replay** |
| `transactions.csv` (1.6 GB) | Pre-2017 transactions | Training snapshot |
| `transactions_v2.csv` (110 MB) | 2017 transactions | **Streaming replay** |

## Topics
| Topic | Source | Key |
|-------|--------|-----|
| `kkbox.user_logs` | `user_logs_v2.csv` | `msno` |
| `kkbox.transactions` | `transactions_v2.csv` | `msno` |

## Architecture
```
GCS Bronze (_v2 CSVs) → Kafka → Consumer → Spark → Silver → Gold → Feast Redis
```

## Run

```bash
# Start Kafka
docker-compose up -d kafka

# Dry run producer — test GCS reads mà không cần Kafka
python data_pipeline/ingestion/kafka_producer.py --dry-run

# Full speed replay
python data_pipeline/ingestion/kafka_producer.py

# Slow replay (1 giây/ngày) để observe consumer
python data_pipeline/ingestion/kafka_producer.py --speed 1.0

# Dry run consumer — test aggregation mà không ghi BQ/Redis
python data_pipeline/ingestion/kafka_consumer.py --dry-run

# Full consumer (cần FEAST_REPO_PATH trỏ đúng feature_store/)
export FEAST_REPO_PATH=./feature_store
python data_pipeline/ingestion/kafka_consumer.py

# Submit Spark jobs lên Dataproc
make spark-bronze-silver
make spark-silver-gold
```

## Consumer flow (per day)

```
Kafka messages (user_logs + transactions)
        │ buffer by date
        ▼
Aggregate per msno:
  user_logs  → total_log_days, total_secs, avg_daily_secs, num_25..num_unq
  txns       → total_transactions, total_amount_paid, auto_renew_count, cancel_count
  members    → city, bd, gender, registered_via (loaded from BQ at startup)
        │
        ▼
BigQuery Gold: kkbox_gold.features_streaming (append, partitioned by event_timestamp)
        │
        ▼
feast materialize <date> → Redis online store updated
        │
        ▼
FastAPI /predict uses fresh features
```

## BigQuery tables

| Table | Content | Used by |
|---|---|---|
| `features_train` | Training snapshot (cutoff 2016-12-31) | Training + initial materialize |
| `features_streaming` | Streaming updates (2017+) | Consumer writes, incremental materialize |
