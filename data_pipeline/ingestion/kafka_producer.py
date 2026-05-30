"""
Kafka Producer — Historical Playback Streaming

Replays Bronze-layer _v2 CSV files (2017 data) to Kafka topics in
chronological order, simulating real-time data ingestion.

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
import time
from collections import defaultdict
from datetime import date, datetime

import pandas as pd
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

GCS_BUCKET     = os.getenv("GCS_BUCKET",     "kkbox-churn-prediction-493716-data")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "kkbox-churn-prediction-493716")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

GCS_USER_LOGS    = f"gs://{GCS_BUCKET}/bronze/raw/user_logs_v2.csv"
GCS_TRANSACTIONS = f"gs://{GCS_BUCKET}/bronze/raw/transactions_v2.csv"

DATE_FORMAT = "%Y%m%d"
STREAM_START_DATE = date(2017, 3, 1)

TOPIC_USER_LOGS    = "kkbox.user_logs"
TOPIC_TRANSACTIONS = "kkbox.transactions"

# Optimized dtypes — keeps each chunk lean in RAM
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

def _gcs_chunks(gcs_url: str, chunksize: int, **read_kwargs):
    """Yield pandas DataFrame chunks streamed from a GCS URL — no local disk used."""
    log.info("Streaming %s in %d-row chunks ...", gcs_url, chunksize)
    return pd.read_csv(gcs_url, chunksize=chunksize, **read_kwargs)


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
    # ── Pre-load transactions — small file (~110 MB on disk, ~25 MB as pandas)
    log.info("Pre-loading transactions from GCS ...")
    txns_by_date: dict[date, list[pd.DataFrame]] = defaultdict(list)
    tx_total = 0
    for chunk in _gcs_chunks(GCS_TRANSACTIONS, 50_000, dtype=_TX_DTYPES):
        chunk["transaction_date"] = pd.to_datetime(
            chunk["transaction_date"].astype(str), format=DATE_FORMAT
        ).dt.date
        chunk = chunk[chunk["transaction_date"] >= STREAM_START_DATE]
        tx_total += len(chunk)
        for d, grp in chunk.groupby("transaction_date"):
            txns_by_date[d].append(grp.reset_index(drop=True))
    gc.collect()
    log.info("Loaded %d transaction rows across %d dates", tx_total, len(txns_by_date))

    producer = None if dry_run else _make_producer()
    total_logs = 0
    total_txn  = 0

    # Accumulate current date's user_log DataFrames (one day at a time)
    current_date: date | None = None
    ul_parts: list[pd.DataFrame] = []

    def _send_date(d: date) -> None:
        nonlocal total_logs, total_txn, ul_parts
        log.info("--- %s ---", d)

        # Send user_logs one 200k-row part at a time — never converts all rows to
        # dicts simultaneously, keeping peak memory low
        ul_count = 0
        for part in ul_parts:
            records = part.to_dict(orient="records")
            for rec in records:
                rec["date"] = d.isoformat()
                if not dry_run:
                    _send(producer, TOPIC_USER_LOGS, rec.get("msno"), rec)
            ul_count += len(records)
        ul_parts = []
        total_logs += ul_count
        log.info("  user_logs:    %d", ul_count)

        # Send transactions
        if d in txns_by_date:
            tx_count = 0
            for part in txns_by_date.pop(d):
                records = part.to_dict(orient="records")
                for rec in records:
                    rec["transaction_date"] = d.isoformat()
                    if not dry_run:
                        _send(producer, TOPIC_TRANSACTIONS, rec.get("msno"), rec)
                tx_count += len(records)
            total_txn += tx_count
            log.info("  transactions: %d", tx_count)

        if producer:
            producer.flush()
        if speed > 0:
            time.sleep(speed)

    # ── Stream user_logs from GCS, flushing one day at a time ─────────────
    for chunk in _gcs_chunks(GCS_USER_LOGS, 200_000, dtype=_UL_DTYPES):
        chunk["date"] = pd.to_datetime(
            chunk["date"].astype(str), format=DATE_FORMAT
        ).dt.date
        chunk = chunk[chunk["date"] >= STREAM_START_DATE]

        for d, grp in chunk.groupby("date"):
            if current_date is None:
                current_date = d
            elif d != current_date:
                _send_date(current_date)
                current_date = d
            ul_parts.append(grp.reset_index(drop=True))

    # Flush final user_log date
    if current_date:
        _send_date(current_date)

    # Send any transaction-only dates (no user_log activity)
    for d in sorted(txns_by_date):
        ul_parts = []
        current_date = d
        _send_date(d)

    if producer:
        producer.close()

    log.info("=== Replay complete ===")
    log.info("Total user_log records  : %d", total_logs)
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
