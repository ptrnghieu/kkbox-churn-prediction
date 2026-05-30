"""
Kafka Producer — Historical Playback Streaming

Replays Bronze-layer _v2 CSV files (2017 data) to Kafka topics in
chronological order, simulating real-time data ingestion.

Architecture:
  GCS Bronze (_v2 CSVs) → Kafka → Consumer → GCS Bronze → Spark → Silver → Gold → Feast

Topics:
  kkbox.user_logs    — daily listening activity per user
  kkbox.transactions — subscription transactions per user

Bronze files streamed:
  bronze/raw/user_logs_v2.csv    (1.3 GB — 2017 listening logs)
  bronze/raw/transactions_v2.csv (110 MB — 2017 transactions)
"""
from __future__ import annotations

import argparse
import gc
import json
import logging
import os
import tempfile
import time
from collections import defaultdict
from datetime import date, datetime

import pandas as pd
from google.cloud import storage as gcs
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

GCS_BUCKET = os.getenv("GCS_BUCKET", "kkbox-churn-prediction-493716-data")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "kkbox-churn-prediction-493716")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_USER_LOGS = "kkbox.user_logs"
TOPIC_TRANSACTIONS = "kkbox.transactions"

BRONZE_PREFIX = "bronze/raw"
USER_LOGS_V2 = f"{BRONZE_PREFIX}/user_logs_v2.csv"
TRANSACTIONS_V2 = f"{BRONZE_PREFIX}/transactions_v2.csv"

DATE_FORMAT = "%Y%m%d"
STREAM_START_DATE = date(2017, 3, 1)

# Optimized dtypes — reduces user_logs RAM from ~4 GB to ~2 GB
_UL_DTYPES = {
    "num_25":    "int32",
    "num_50":    "int32",
    "num_75":    "int32",
    "num_985":   "int32",
    "num_100":   "int32",
    "num_unq":   "int32",
    "total_secs": "float32",
}
_TX_DTYPES = {
    "actual_amount_paid": "float32",
    "plan_list_price":    "float32",
}


# ── Serialization ─────────────────────────────────────────────────────────────

def _serialize(value: dict) -> bytes:
    def _default(obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if pd.isna(obj):
            return None
        raise TypeError(f"Not serializable: {type(obj)}")
    return json.dumps(value, default=_default).encode("utf-8")


# ── GCS helpers ───────────────────────────────────────────────────────────────

def _download_to_disk(client: gcs.Client, blob_path: str) -> str:
    """Stream blob to a temp file on disk — avoids holding raw bytes in RAM."""
    log.info("Downloading gs://%s/%s to disk ...", GCS_BUCKET, blob_path)
    bucket = client.bucket(GCS_BUCKET)
    fd, tmp_path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    bucket.blob(blob_path).download_to_filename(tmp_path)
    size_mb = os.path.getsize(tmp_path) / 1024 / 1024
    log.info("Downloaded %.0f MB → %s", size_mb, tmp_path)
    return tmp_path


# ── Producer helpers ──────────────────────────────────────────────────────────

def _make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=_serialize,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=1,
        retries=3,
        linger_ms=50,
        batch_size=65536,
    )


def _send(producer: KafkaProducer, topic: str, key: str, record: dict) -> None:
    producer.send(topic, key=key, value=record)


# ── Replay logic ──────────────────────────────────────────────────────────────

def replay(speed: float, dry_run: bool) -> None:
    gcs_client = gcs.Client(project=GCP_PROJECT_ID)

    # Download to temp disk files — avoids keeping 1.3 GB in RAM as bytes
    ul_path = _download_to_disk(gcs_client, USER_LOGS_V2)
    tx_path = _download_to_disk(gcs_client, TRANSACTIONS_V2)

    try:
        # ── Chunked loading — avoids pandas parsing the full 1.3 GB at once ─
        log.info("Loading user_logs in chunks (200 k rows each) ...")
        logs_by_date: dict[date, list] = defaultdict(list)
        ul_rows = 0
        for chunk in pd.read_csv(ul_path, dtype=_UL_DTYPES, chunksize=200_000):
            chunk["date"] = pd.to_datetime(
                chunk["date"].astype(str), format=DATE_FORMAT
            ).dt.date
            chunk = chunk[chunk["date"] >= STREAM_START_DATE]
            ul_rows += len(chunk)
            for d, grp in chunk.groupby("date"):
                logs_by_date[d].append(grp.reset_index(drop=True))
        gc.collect()

        log.info("Loading transactions in chunks (50 k rows each) ...")
        txns_by_date: dict[date, list] = defaultdict(list)
        tx_rows = 0
        for chunk in pd.read_csv(tx_path, dtype=_TX_DTYPES, chunksize=50_000):
            chunk["transaction_date"] = pd.to_datetime(
                chunk["transaction_date"].astype(str), format=DATE_FORMAT
            ).dt.date
            chunk = chunk[chunk["transaction_date"] >= STREAM_START_DATE]
            tx_rows += len(chunk)
            for d, grp in chunk.groupby("transaction_date"):
                txns_by_date[d].append(grp.reset_index(drop=True))
        gc.collect()

        log.info(
            "After filter (>= %s): user_logs=%d rows, transactions=%d rows",
            STREAM_START_DATE, ul_rows, tx_rows,
        )

        all_dates = sorted(set(logs_by_date) | set(txns_by_date))
        log.info(
            "Dates to replay: %d  (%s → %s)",
            len(all_dates), all_dates[0], all_dates[-1],
        )

        producer = None if dry_run else _make_producer()

        total_logs = 0
        total_txn = 0

        for current_date in all_dates:
            log.info("--- %s ---", current_date)

            # concat + pop frees each date's chunk list from memory after sending
            if current_date in logs_by_date:
                df = pd.concat(logs_by_date.pop(current_date), ignore_index=True)
                records = df.to_dict(orient="records")
                del df
                for record in records:
                    record["date"] = current_date.isoformat()
                    if not dry_run:
                        _send(producer, TOPIC_USER_LOGS, record.get("msno"), record)
                total_logs += len(records)
                log.info("  user_logs:    %d", len(records))

            if current_date in txns_by_date:
                df = pd.concat(txns_by_date.pop(current_date), ignore_index=True)
                records = df.to_dict(orient="records")
                del df
                for record in records:
                    record["transaction_date"] = current_date.isoformat()
                    if not dry_run:
                        _send(producer, TOPIC_TRANSACTIONS, record.get("msno"), record)
                total_txn += len(records)
                log.info("  transactions: %d", len(records))

            if producer:
                producer.flush()
            if speed > 0:
                time.sleep(speed)

        if producer:
            producer.flush()
            producer.close()

        log.info("=== Replay complete ===")
        log.info("Total user_log records  : %d", total_logs)
        log.info("Total transaction records: %d", total_txn)

    finally:
        for p in [ul_path, tx_path]:
            try:
                os.unlink(p)
            except OSError:
                pass


# ── Entry point ───────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="KKBox historical playback Kafka producer")
    p.add_argument(
        "--speed",
        type=float,
        default=0.0,
        help="Seconds to sleep between days. 0 = full speed (default: 0)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Log messages without sending to Kafka",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    log.info(
        "Starting historical playback: speed=%.1fs  dry_run=%s",
        args.speed,
        args.dry_run,
    )
    replay(speed=args.speed, dry_run=args.dry_run)
