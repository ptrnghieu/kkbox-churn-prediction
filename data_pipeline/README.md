# Data Pipeline

## Responsibilities
- Kafka simulated streaming từ GCS CSV
- Spark Bronze→Silver→Gold transformation

## Files
- `ingestion/kafka_producer.py` — Replay CSV → Kafka topics theo thứ tự date
- `ingestion/kafka_consumer.py` — Kafka → GCS Bronze
- `processing/bronze_to_silver.py` — Spark: clean, cast, deduplicate
- `processing/silver_to_gold.py` — Spark: feature aggregation, join

## Run
```bash
# Submit Bronze→Silver
make spark-bronze-silver

# Submit Silver→Gold
make spark-silver-gold
```
