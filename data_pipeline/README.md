# Data Pipeline

Pipeline xử lý dữ liệu KKBox gồm hai phần: **Kafka historical playback** để giả lập streaming thực tế, và **Spark batch jobs** để transform Bronze → Silver → Gold (dùng cho initial data prep).

## Responsibilities

- Stream dữ liệu 2017 (`_v2` files) từ GCS Bronze qua Kafka theo thứ tự từng ngày
- Consume từ Kafka, aggregate cumulative features per user per day, ghi vào BigQuery Gold, materialize vào Redis qua Feast
- Spark batch jobs để transform Bronze → Silver → Gold (chạy một lần, không dùng hàng ngày)

## File Structure

```
data_pipeline/
├── ingestion/
│   ├── kafka_producer.py    -- Replay _v2 CSVs từ GCS → Kafka (zero disk)
│   └── kafka_consumer.py    -- Kafka → BigQuery Gold → Feast Redis
└── processing/
    ├── bronze_to_silver.py  -- Spark: clean, cast, deduplicate
    └── silver_to_gold.py    -- Spark: feature aggregation, join với members
```

## Bronze Data Files (GCS)

| File | Kích thước | Dùng cho |
|------|-----------|----------|
| `user_logs.csv` | 28.4 GB | Training snapshot (pre-2017) |
| `user_logs_v2.csv` | 1.3 GB | Streaming replay (2017) |
| `transactions.csv` | 1.6 GB | Training snapshot (pre-2017) |
| `transactions_v2.csv` | 110 MB | Streaming replay (2017) |

Tất cả đều nằm trong GCS bucket `gs://kkbox-churn-prediction-493716-data/`.

## Kafka Topics

| Topic | Source | Message key |
|-------|--------|-------------|
| `kkbox.user_logs` | `user_logs_v2.csv` | `msno` |
| `kkbox.transactions` | `transactions_v2.csv` | `msno` |

Kafka chạy ở chế độ KRaft (không cần ZooKeeper). Hai listener:
- `PLAINTEXT://kafka:9092` — internal Docker network
- `PLAINTEXT_HOST://localhost:9093` — host access (dùng bởi producer/consumer khi chạy ngoài Docker)

Sau khi gửi hết dữ liệu của một ngày, producer gửi **EOD marker** (End-of-Day) trên cả hai topics. Consumer chờ EOD từ cả hai mới flush ngày đó.

## kafka_producer.py

Streams trực tiếp từ GCS bằng `gcsfs` — không ghi xuống disk (zero local storage).

**Cách hoạt động:**
- Pre-load `transactions_v2.csv` vào memory (~25MB) tại startup
- Stream `user_logs_v2.csv` theo từng chunk 200K rows
- Với mỗi ngày: gửi hết messages của ngày đó (cả user_logs và transactions), rồi gửi EOD marker trên cả hai topics
- Sleep `--speed` giây giữa các ngày

```bash
# Dry run — test đọc GCS, không cần Kafka running
python data_pipeline/ingestion/kafka_producer.py --dry-run

# Full speed replay (không nghỉ giữa các ngày)
python data_pipeline/ingestion/kafka_producer.py

# Slow replay — 55 giây/ngày (khuyến nghị cho streaming simulation)
python data_pipeline/ingestion/kafka_producer.py --speed 55

# Xem tất cả options
python data_pipeline/ingestion/kafka_producer.py --help
```

## kafka_consumer.py

Thường được FastAPI spawn tự động qua endpoint `POST /stream/start`. Có thể chạy standalone.

**Luồng xử lý:**

1. Tạo `KafkaConsumer` với `auto_offset_reset=latest` **trước** khi load BQ profiles — để lock offset, tránh miss messages khi producer bắt đầu sau
2. Sleep 20 giây (FastAPI cũng delay 20s trước khi khởi động producer)
3. Load member profiles từ BigQuery: `city`, `bd`, `gender`, `registered_via`
4. Consume messages, buffer theo ngày
5. Chờ EOD marker trên **cả hai** topics trước khi flush
6. Gửi `POST /stream/notify` tới FastAPI (non-blocking, trước khi ghi BQ)
7. Background thread: BQ write + Feast materialize (không block consumer loop)

**Cumulative aggregation:**

```
features = baseline từ features_train (snapshot 2016-12-31)
         + streaming delta (tích lũy trong memory qua các ngày)
```

Mỗi ngày mới, delta được cộng dồn vào baseline để tạo ra feature vector mới nhất cho từng user.

```bash
# Dry run — không ghi BQ/Redis
python data_pipeline/ingestion/kafka_consumer.py --dry-run

# Full run
export FEAST_REPO_PATH=./feature_store
python data_pipeline/ingestion/kafka_consumer.py \
  --bootstrap-servers localhost:9093 \
  --notify-url http://localhost:8000/stream/notify
```

## Consumer Flow (per day)

```
Kafka messages (kkbox.user_logs + kkbox.transactions)
        │ buffer by date
        ▼
Aggregate per msno:
  user_logs    → total_log_days, total_secs, avg_daily_secs,
                 num_25, num_50, num_75, num_985, num_100, num_unq
  transactions → total_transactions, total_amount_paid,
                 auto_renew_count, cancel_count, avg_amount_paid
  members      → city, bd, gender, registered_via  (loaded at startup)
        │
        ▼ cumulative: features = features_train baseline + streaming delta
        │
        ▼ POST /stream/notify  →  FastAPI cập nhật current_date + user list
        │
        ▼ background thread (non-blocking)
BigQuery Gold: kkbox_gold.features_streaming
  (append-only, partitioned by event_timestamp)
        │
        ▼ feast materialize
Redis (Cloud Memorystore 10.80.68.19:6379) — latest features per msno
        │
        ▼ FastAPI /predict/ sử dụng features mới nhất
```

## BigQuery Tables

| Table | Nội dung | Ghi bởi |
|-------|----------|---------|
| `kkbox_gold.features_train` | Training snapshot (cutoff 2016-12-31), ~1M rows | Spark silver_to_gold |
| `kkbox_gold.features_streaming` | Streaming updates 2017, append-only, partitioned by event_timestamp | kafka_consumer.py |
| `kkbox_gold.members` | Member profiles: city, bd, gender, registered_via | Spark silver_to_gold |

## Spark Jobs (Dataproc)

Dùng cho initial data preparation. Không chạy trong streaming loop hàng ngày.

```bash
# Bronze → Silver (clean, cast, deduplicate)
make spark-bronze-silver

# Silver → Gold (feature aggregation, join với members)
make spark-silver-gold
```

Spark jobs đọc từ GCS Bronze, ghi ra GCS Silver/Gold, sau đó load vào BigQuery.

## Dependencies

| Package | Mục đích |
|---------|---------|
| `gcsfs` | Đọc GCS trực tiếp không cần gsutil, zero disk |
| `confluent-kafka` | Kafka producer/consumer |
| `google-cloud-bigquery` | Ghi vào BigQuery |
| `feast` | Materialize features vào Redis |
| `pyspark` | Spark batch processing (chỉ dùng cho Spark jobs) |

## Environment Variables

| Variable | Default | Mô tả |
|----------|---------|-------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `GCP_PROJECT_ID` | `kkbox-churn-prediction-493716` | GCP project |
| `FEAST_REPO_PATH` | `./feature_store` | Đường dẫn tới thư mục `feature_store/` |
| `FASTAPI_BASE_URL` | `http://localhost:8000` | URL của FastAPI server |

## Lưu ý khi chạy trên VM

Kafka được cấu hình với hai listeners:
- Port **9092**: dùng trong Docker network (container-to-container)
- Port **9093**: dùng từ host (producer/consumer Python scripts)

Khi chạy producer/consumer trực tiếp trên VM (không trong Docker), dùng `--bootstrap-servers localhost:9093`.

FastAPI `/stream/start` tự động set `KAFKA_BOOTSTRAP_SERVERS=localhost:9093` khi spawn consumer.
