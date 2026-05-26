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
import io
import json
import logging
import os
import time
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

# Date column values in Bronze are INTEGER yyyyMMdd
DATE_FORMAT = "%Y%m%d"

# Stream from 2017-03-01: first date where both user_logs AND transactions
# are available. Jan-Feb 2017 has transactions only (user_logs_v2 starts March).
STREAM_START_DATE = date(2017, 3, 1)


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

def _download_csv(client: gcs.Client, blob_path: str) -> pd.DataFrame:
    log.info("Downloading gs://%s/%s ...", GCS_BUCKET, blob_path)
    bucket = client.bucket(GCS_BUCKET)
    data = bucket.blob(blob_path).download_as_bytes()
    df = pd.read_csv(io.BytesIO(data))
    log.info("Downloaded %s — shape: %s", blob_path.split("/")[-1], df.shape)
    return df


# ── Producer helpers ──────────────────────────────────────────────────────────

def _make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=_serialize,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def _send(producer: KafkaProducer, topic: str, key: str, record: dict) -> None:
    future = producer.send(topic, key=key, value=record)
    try:
        future.get(timeout=10)
    except KafkaError:
        log.exception("Failed to send to %s key=%s", topic, key)


# ── Replay logic ──────────────────────────────────────────────────────────────

def replay(speed: float, dry_run: bool) -> None:
    """
    Main replay loop — process one day at a time in chronological order.

    Args:
        speed:   Seconds to sleep between days. 0 = full speed.
        dry_run: Log messages without sending to Kafka.
    """
    gcs_client = gcs.Client(project=GCP_PROJECT_ID)

    # Load both files upfront (1.3 GB + 110 MB)
    user_logs = _download_csv(gcs_client, USER_LOGS_V2)
    transactions = _download_csv(gcs_client, TRANSACTIONS_V2)

    # Parse date columns (INTEGER yyyyMMdd → date)
    user_logs["date"] = pd.to_datetime(user_logs["date"].astype(str), format=DATE_FORMAT).dt.date
    transactions["transaction_date"] = pd.to_datetime(
        transactions["transaction_date"].astype(str), format=DATE_FORMAT
    ).dt.date

    # Filter to post-training data only (transactions_v2 contains pre-2017 history too)
    transactions = transactions[transactions["transaction_date"] >= STREAM_START_DATE]
    user_logs = user_logs[user_logs["date"] >= STREAM_START_DATE]
    log.info(
        "After filter (>= %s): user_logs=%d rows, transactions=%d rows",
        STREAM_START_DATE, len(user_logs), len(transactions),
    )


    # Build per-date groups
    logs_by_date = dict(tuple(user_logs.groupby("date")))
    txn_by_date = dict(tuple(transactions.groupby("transaction_date")))

    all_dates = sorted(set(logs_by_date) | set(txn_by_date))
    log.info("Dates to replay: %d  (%s → %s)", len(all_dates), all_dates[0], all_dates[-1])

    producer = None if dry_run else _make_producer()

    total_logs = 0
    total_txn = 0

    for current_date in all_dates:
        log.info("--- %s ---", current_date)

        # user_logs
        if current_date in logs_by_date:
            records = logs_by_date[current_date].to_dict(orient="records")
            for record in records:
                record["date"] = current_date.isoformat()
                if not dry_run:
                    _send(producer, TOPIC_USER_LOGS, record.get("msno"), record)
            total_logs += len(records)
            log.info("  user_logs:    %d", len(records))

        # transactions
        if current_date in txn_by_date:
            records = txn_by_date[current_date].to_dict(orient="records")
            for record in records:
                record["transaction_date"] = current_date.isoformat()
                if not dry_run:
                    _send(producer, TOPIC_TRANSACTIONS, record.get("msno"), record)
            total_txn += len(records)
            log.info("  transactions: %d", len(records))

        if speed > 0:
            time.sleep(speed)

    if producer:
        producer.flush()
        producer.close()

    log.info("=== Replay complete ===")
    log.info("Total user_log records : %d", total_logs)
    log.info("Total transaction records: %d", total_txn)


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
