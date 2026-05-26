"""
Kafka Producer — Historical Playback Streaming

Replays Silver-layer data after START_DATE to Kafka topics in chronological
order, simulating a real-time streaming environment.

Topics:
  kkbox.user_logs    — daily listening activity per user
  kkbox.transactions — subscription transactions per user

Silver layout on GCS:
  silver/user_logs/date=YYYY-MM-DD/*.parquet   (partitioned by date)
  silver/transactions/*.parquet                 (filtered by transaction_date)
"""
from __future__ import annotations

import argparse
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

SILVER_USER_LOGS_PREFIX = "silver/user_logs"
SILVER_TRANSACTIONS_PREFIX = "silver/transactions"

# Data before this date is used for training (features_train snapshot)
PLAYBACK_START_DATE = date(2017, 1, 1)


# ── Serialization ─────────────────────────────────────────────────────────────

def _serialize(value: dict) -> bytes:
    """Serialize a dict to JSON bytes, handling date/NaN/NA types."""
    def _default(obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if pd.isna(obj):
            return None
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    return json.dumps(value, default=_default).encode("utf-8")


# ── GCS helpers ───────────────────────────────────────────────────────────────

def _list_user_log_dates(client: gcs.Client, start: date) -> list[date]:
    """Return sorted list of dates in silver/user_logs/ that are >= start."""
    bucket = client.bucket(GCS_BUCKET)
    blobs = client.list_blobs(GCS_BUCKET, prefix=f"{SILVER_USER_LOGS_PREFIX}/date=")
    seen: set[date] = set()
    for blob in blobs:
        # blob.name: silver/user_logs/date=2017-01-15/part-0000.parquet
        part = blob.name.split("/")
        for segment in part:
            if segment.startswith("date="):
                try:
                    d = date.fromisoformat(segment[5:])
                    if d >= start:
                        seen.add(d)
                except ValueError:
                    pass
    return sorted(seen)


def _read_parquet_prefix(client: gcs.Client, prefix: str) -> pd.DataFrame:
    """Download all parquet files under a GCS prefix and concatenate."""
    import io
    import pyarrow.parquet as pq

    bucket = client.bucket(GCS_BUCKET)
    blobs = [b for b in client.list_blobs(GCS_BUCKET, prefix=prefix) if b.name.endswith(".parquet")]
    if not blobs:
        return pd.DataFrame()

    frames = []
    for blob in blobs:
        data = blob.download_as_bytes()
        frames.append(pq.read_table(io.BytesIO(data)).to_pandas())
    return pd.concat(frames, ignore_index=True)


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
        log.exception("Failed to send message to %s key=%s", topic, key)


# ── Replay logic ──────────────────────────────────────────────────────────────

def replay(start: date, speed: float, dry_run: bool) -> None:
    """
    Main replay loop — process one day at a time.

    Args:
        start:   First date to replay (default 2017-01-01).
        speed:   Seconds to sleep between days. 0 = full speed.
        dry_run: If True, log messages without sending to Kafka.
    """
    gcs_client = gcs.Client(project=GCP_PROJECT_ID)

    log.info("Discovering date partitions in Silver user_logs >= %s ...", start)
    dates = _list_user_log_dates(gcs_client, start)
    log.info("Found %d date partitions to replay", len(dates))
    if not dates:
        log.warning("No data found after %s — nothing to replay.", start)
        return

    log.info("Loading transactions from Silver (filter >= %s)...", start)
    txn_all = _read_parquet_prefix(gcs_client, SILVER_TRANSACTIONS_PREFIX)
    if not txn_all.empty:
        txn_all["transaction_date"] = pd.to_datetime(txn_all["transaction_date"]).dt.date
        txn_all = txn_all[txn_all["transaction_date"] >= start].sort_values("transaction_date")
        txn_by_date = dict(tuple(txn_all.groupby("transaction_date")))
        log.info("Transactions to replay: %d across %d dates", len(txn_all), len(txn_by_date))
    else:
        txn_by_date = {}
        log.warning("No transactions found in Silver layer.")

    producer = None if dry_run else _make_producer()

    total_logs_sent = 0
    total_txn_sent = 0

    for current_date in dates:
        log.info("--- Replaying date: %s ---", current_date)

        # user_logs for this date
        prefix = f"{SILVER_USER_LOGS_PREFIX}/date={current_date}"
        logs_df = _read_parquet_prefix(gcs_client, prefix)
        for record in logs_df.to_dict(orient="records"):
            record["date"] = current_date.isoformat()
            if dry_run:
                log.debug("DRY-RUN user_log: msno=%s", record.get("msno"))
            else:
                _send(producer, TOPIC_USER_LOGS, record.get("msno"), record)
        total_logs_sent += len(logs_df)
        log.info("  user_logs sent: %d", len(logs_df))

        # transactions for this date
        if current_date in txn_by_date:
            txn_today = txn_by_date[current_date]
            for record in txn_today.to_dict(orient="records"):
                record["transaction_date"] = current_date.isoformat()
                if dry_run:
                    log.debug("DRY-RUN transaction: msno=%s", record.get("msno"))
                else:
                    _send(producer, TOPIC_TRANSACTIONS, record.get("msno"), record)
            total_txn_sent += len(txn_today)
            log.info("  transactions sent: %d", len(txn_today))

        if speed > 0:
            log.info("  Sleeping %.1fs before next day...", speed)
            time.sleep(speed)

    if producer:
        producer.flush()
        producer.close()

    log.info("=== Replay complete ===")
    log.info("Total user_log records sent: %d", total_logs_sent)
    log.info("Total transaction records sent: %d", total_txn_sent)


# ── Entry point ───────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="KKBox historical playback Kafka producer")
    p.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=PLAYBACK_START_DATE,
        help="First date to replay (default: 2017-01-01)",
    )
    p.add_argument(
        "--speed",
        type=float,
        default=0.0,
        help="Seconds to sleep between days. 0 = full speed (default: 0)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Log messages without actually sending to Kafka",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    log.info(
        "Starting historical playback: start=%s  speed=%.1fs  dry_run=%s",
        args.start_date,
        args.speed,
        args.dry_run,
    )
    replay(start=args.start_date, speed=args.speed, dry_run=args.dry_run)
