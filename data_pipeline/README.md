# Data Pipeline

## Responsibilities
- Kafka historical playback streaming từ Silver layer
- Spark Bronze→Silver→Gold transformation

## Files
- `ingestion/kafka_producer.py` — Replay Silver Parquet (post 2017-01-01) → Kafka topics theo thứ tự date
- `processing/bronze_to_silver.py` — Spark: clean, cast, deduplicate
- `processing/silver_to_gold.py` — Spark: feature aggregation, join

## Topics
| Topic | Nội dung | Key |
|-------|----------|-----|
| `kkbox.user_logs` | Daily listening activity per user | `msno` |
| `kkbox.transactions` | Subscription transactions | `msno` |

## Run

```bash
# Start Kafka (Docker)
docker-compose up -d kafka

# Full speed replay từ 2017-01-01
python data_pipeline/ingestion/kafka_producer.py

# Replay chậm lại (1 giây/ngày) để observe
python data_pipeline/ingestion/kafka_producer.py --speed 1.0

# Dry run — không gửi Kafka, chỉ log
python data_pipeline/ingestion/kafka_producer.py --dry-run

# Từ ngày cụ thể
python data_pipeline/ingestion/kafka_producer.py --start-date 2017-03-01

# Submit Spark jobs lên Dataproc
make spark-bronze-silver
make spark-silver-gold
```
