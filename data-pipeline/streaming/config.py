import os

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: list[str] = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9192,localhost:9292",
).split(",")

# One topic per data source (raw/bronze messages)
TOPICS = {
    "ilinet":    "disease-ilinet-raw",
    "covidcast": "disease-covidcast-raw",
    "flusurv":   "disease-flusurv-raw",
    "noaa":      "disease-noaa-raw",
}

CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_ID", "disease-pipeline-consumer")

# ---------------------------------------------------------------------------
# MinIO (S3-compatible)
# ---------------------------------------------------------------------------
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minioadmin")
MINIO_BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW",  "disease-raw")
