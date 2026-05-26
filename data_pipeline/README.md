# Data Pipeline

## Responsibilities
- Kafka historical playback streaming từ Bronze `_v2` files (2017 data)
- Spark Bronze→Silver→Gold transformation

## Files
- `ingestion/kafka_producer.py` — Replay Bronze `_v2` CSVs → Kafka topics theo thứ tự date
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

# Dry run — test GCS reads mà không cần Kafka
python data_pipeline/ingestion/kafka_producer.py --dry-run

# Full speed replay
python data_pipeline/ingestion/kafka_producer.py

# Slow replay (1 giây/ngày) để observe consumer
python data_pipeline/ingestion/kafka_producer.py --speed 1.0

# Submit Spark jobs lên Dataproc
make spark-bronze-silver
make spark-silver-gold
```
