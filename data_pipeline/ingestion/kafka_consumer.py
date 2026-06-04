"""
Kafka Consumer — Streaming Feature Aggregation

Reads from kkbox.user_logs and kkbox.transactions topics, aggregates
features per user per day, writes to BigQuery Gold, and triggers
Feast materialize to update Redis online store.

Flow:
  Kafka → Consumer → aggregate per day → BigQuery Gold → feast materialize → Redis

Topics consumed:
  kkbox.user_logs       — daily listening activity (key=msno)
  kkbox.transactions    — subscription transactions  (key=msno)

BigQuery output:
  kkbox_gold.features_streaming  — append-only, one row per (msno, date)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from feast import FeatureStore
from google.cloud import bigquery
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

GCP_PROJECT_ID  = os.getenv("GCP_PROJECT_ID",  "kkbox-churn-prediction-493716")
BQ_DATASET      = os.getenv("BQ_DATASET",      "kkbox_gold")
STREAM_TABLE    = os.getenv("STREAM_TABLE",     "features_streaming")
FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH",  "./feature_store")

TOPIC_USER_LOGS    = "kkbox.user_logs"
TOPIC_TRANSACTIONS = "kkbox.transactions"

FULL_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{STREAM_TABLE}"

# ── BigQuery schema ───────────────────────────────────────────────────────────

BQ_SCHEMA = [
    bigquery.SchemaField("msno",               "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("event_timestamp",    "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("city",               "INTEGER"),
    bigquery.SchemaField("bd",                 "INTEGER"),
    bigquery.SchemaField("gender",             "STRING"),
    bigquery.SchemaField("registered_via",     "INTEGER"),
    bigquery.SchemaField("total_transactions", "INTEGER"),
    bigquery.SchemaField("total_amount_paid",  "FLOAT"),
    bigquery.SchemaField("avg_amount_paid",    "FLOAT"),
    bigquery.SchemaField("auto_renew_count",   "INTEGER"),
    bigquery.SchemaField("cancel_count",       "INTEGER"),
    bigquery.SchemaField("total_log_days",     "INTEGER"),
    bigquery.SchemaField("total_secs",         "FLOAT"),
    bigquery.SchemaField("avg_daily_secs",     "FLOAT"),
    bigquery.SchemaField("total_num_25",       "INTEGER"),
    bigquery.SchemaField("total_num_50",       "INTEGER"),
    bigquery.SchemaField("total_num_75",       "INTEGER"),
    bigquery.SchemaField("total_num_985",      "INTEGER"),
    bigquery.SchemaField("total_num_100",      "INTEGER"),
    bigquery.SchemaField("total_num_unq",      "INTEGER"),
]


# ── Member cache ──────────────────────────────────────────────────────────────

def load_members(bq: bigquery.Client) -> dict[str, dict]:
    """Load full feature baseline from features_train into memory."""
    log.info("Loading member profiles from BigQuery...")
    query = f"""
        SELECT msno, city, bd, gender, registered_via,
               total_transactions, total_amount_paid, auto_renew_count, cancel_count,
               total_log_days, total_secs,
               total_num_25, total_num_50, total_num_75, total_num_985, total_num_100, total_num_unq
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.features_train`
    """
    rows = bq.query(query).result()
    members = {}
    for row in rows:
        members[row.msno] = {
            "city":               row.city,
            "bd":                 row.bd,
            "gender":             row.gender,
            "registered_via":     row.registered_via,
            "total_transactions": row.total_transactions or 0,
            "total_amount_paid":  float(row.total_amount_paid or 0),
            "auto_renew_count":   row.auto_renew_count or 0,
            "cancel_count":       row.cancel_count or 0,
            "total_log_days":     row.total_log_days or 0,
            "total_secs":         float(row.total_secs or 0),
            "total_num_25":       row.total_num_25 or 0,
            "total_num_50":       row.total_num_50 or 0,
            "total_num_75":       row.total_num_75 or 0,
            "total_num_985":      row.total_num_985 or 0,
            "total_num_100":      row.total_num_100 or 0,
            "total_num_unq":      row.total_num_unq or 0,
        }
    log.info("Loaded %d member profiles", len(members))
    return members


# ── Aggregation ───────────────────────────────────────────────────────────────

def _aggregate_logs(records: list[dict]) -> dict:
    """Aggregate user_log records for one msno over one day."""
    total_secs = sum(r.get("total_secs") or 0 for r in records)
    return {
        "total_log_days": len(records),
        "total_secs": total_secs,
        "avg_daily_secs": total_secs / len(records) if records else None,
        "total_num_25":  sum(r.get("num_25")  or 0 for r in records),
        "total_num_50":  sum(r.get("num_50")  or 0 for r in records),
        "total_num_75":  sum(r.get("num_75")  or 0 for r in records),
        "total_num_985": sum(r.get("num_985") or 0 for r in records),
        "total_num_100": sum(r.get("num_100") or 0 for r in records),
        "total_num_unq": sum(r.get("num_unq") or 0 for r in records),
    }


def _aggregate_txns(records: list[dict]) -> dict:
    """Aggregate transaction records for one msno over one day."""
    amounts = [r.get("actual_amount_paid") or 0 for r in records]
    total = sum(amounts)
    return {
        "total_transactions": len(records),
        "total_amount_paid":  total,
        "avg_amount_paid":    total / len(records) if records else None,
        "auto_renew_count":   sum(r.get("is_auto_renew") or 0 for r in records),
        "cancel_count":       sum(r.get("is_cancel") or 0 for r in records),
    }


def build_rows(
    date_str: str,
    logs_buf: dict[str, list],
    txns_buf: dict[str, list],
    members: dict[str, dict],
) -> tuple[list[dict], dict[str, dict]]:
    """Accumulate baseline + streaming delta into cumulative features.

    Returns (bq_rows, updated_baselines) where updated_baselines should
    be merged back into members so the next day starts from this state.
    """
    all_msno = set(logs_buf) | set(txns_buf)
    event_ts = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    rows: list[dict] = []
    updates: dict[str, dict] = {}

    for msno in all_msno:
        base = members.get(msno, {})
        log_d = _aggregate_logs(logs_buf[msno]) if msno in logs_buf else {}
        txn_d = _aggregate_txns(txns_buf[msno]) if msno in txns_buf else {}

        # Cumulative totals — baseline historical + today's delta
        total_log_days     = (base.get("total_log_days")     or 0) + (log_d.get("total_log_days")     or 0)
        total_secs         = (base.get("total_secs")         or 0) + (log_d.get("total_secs")         or 0)
        total_num_25       = (base.get("total_num_25")       or 0) + (log_d.get("total_num_25")       or 0)
        total_num_50       = (base.get("total_num_50")       or 0) + (log_d.get("total_num_50")       or 0)
        total_num_75       = (base.get("total_num_75")       or 0) + (log_d.get("total_num_75")       or 0)
        total_num_985      = (base.get("total_num_985")      or 0) + (log_d.get("total_num_985")      or 0)
        total_num_100      = (base.get("total_num_100")      or 0) + (log_d.get("total_num_100")      or 0)
        total_num_unq      = (base.get("total_num_unq")      or 0) + (log_d.get("total_num_unq")      or 0)
        total_transactions = (base.get("total_transactions") or 0) + (txn_d.get("total_transactions") or 0)
        total_amount_paid  = (base.get("total_amount_paid")  or 0) + (txn_d.get("total_amount_paid")  or 0)
        auto_renew_count   = (base.get("auto_renew_count")   or 0) + (txn_d.get("auto_renew_count")   or 0)
        cancel_count       = (base.get("cancel_count")       or 0) + (txn_d.get("cancel_count")       or 0)

        row: dict[str, Any] = {
            "msno":               msno,
            "event_timestamp":    event_ts.isoformat(),
            "city":               base.get("city"),
            "bd":                 base.get("bd"),
            "gender":             base.get("gender"),
            "registered_via":     base.get("registered_via"),
            "total_log_days":     total_log_days,
            "total_secs":         total_secs,
            "avg_daily_secs":     total_secs / total_log_days if total_log_days > 0 else None,
            "total_num_25":       total_num_25,
            "total_num_50":       total_num_50,
            "total_num_75":       total_num_75,
            "total_num_985":      total_num_985,
            "total_num_100":      total_num_100,
            "total_num_unq":      total_num_unq,
            "total_transactions": total_transactions,
            "total_amount_paid":  total_amount_paid,
            "avg_amount_paid":    total_amount_paid / total_transactions if total_transactions > 0 else None,
            "auto_renew_count":   auto_renew_count,
            "cancel_count":       cancel_count,
        }
        rows.append(row)

        # Updated baseline for next day's accumulation
        updates[msno] = {**base,
                         "total_log_days": total_log_days, "total_secs": total_secs,
                         "total_num_25": total_num_25, "total_num_50": total_num_50,
                         "total_num_75": total_num_75, "total_num_985": total_num_985,
                         "total_num_100": total_num_100, "total_num_unq": total_num_unq,
                         "total_transactions": total_transactions, "total_amount_paid": total_amount_paid,
                         "auto_renew_count": auto_renew_count, "cancel_count": cancel_count}

    return rows, updates


# ── BigQuery helpers ──────────────────────────────────────────────────────────

def ensure_table(bq: bigquery.Client) -> None:
    """Create the streaming features table if it doesn't exist."""
    table_ref = bigquery.Table(FULL_TABLE, schema=BQ_SCHEMA)
    table_ref.time_partitioning = bigquery.TimePartitioning(field="event_timestamp")
    try:
        bq.get_table(FULL_TABLE)
        log.info("Table %s already exists", FULL_TABLE)
    except Exception:
        bq.create_table(table_ref)
        log.info("Created table %s", FULL_TABLE)


def write_to_bq(bq: bigquery.Client, rows: list[dict], dry_run: bool) -> None:
    if dry_run:
        log.info("[dry-run] Would write %d rows to BigQuery", len(rows))
        return
    errors = bq.insert_rows_json(FULL_TABLE, rows)
    if errors:
        log.error("BigQuery insert errors: %s", errors)
    else:
        log.info("Wrote %d rows to %s", len(rows), FULL_TABLE)


# ── Feast materialize ─────────────────────────────────────────────────────────

def materialize(date_str: str, dry_run: bool) -> None:
    """Push new rows for date_str from BigQuery into Redis."""
    if dry_run:
        log.info("[dry-run] Would run feast materialize for %s", date_str)
        return
    from datetime import timedelta
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start = dt
    end   = dt + timedelta(days=1)
    log.info("Running feast materialize %s → %s", start, end)
    store = FeatureStore(repo_path=FEAST_REPO_PATH)
    store.materialize(start_date=start, end_date=end)
    log.info("Feast materialize complete for %s", date_str)


# ── Consumer ──────────────────────────────────────────────────────────────────

def _notify(url: str, date: str, msnos: list) -> None:
    """POST date + msnos to the stream controller (best-effort)."""
    import urllib.request
    body = json.dumps({"date": date, "msnos": msnos}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as exc:
        log.warning("Failed to notify %s for date %s: %s", url, date, exc)


def consume(bootstrap_servers: str, dry_run: bool,
            notify_url: str = None, group_id: str = "kkbox-feature-consumer",
            auto_offset_reset: str = "earliest",
            idle_timeout_ms: int = 30_000) -> None:
    bq = bigquery.Client(project=GCP_PROJECT_ID)

    # Create KafkaConsumer FIRST so we claim the current "latest" offset
    # before spending 60+ seconds loading member profiles from BigQuery.
    # Messages produced after this poll() call will be visible to the consumer.
    consumer = KafkaConsumer(
        TOPIC_USER_LOGS,
        TOPIC_TRANSACTIONS,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=idle_timeout_ms,
        # BQ write + Feast materialize can take several minutes per day;
        # set max_poll_interval to 30 min so the consumer isn't kicked mid-flush
        max_poll_interval_ms=1_800_000,
    )
    # Force partition assignment + offset seek NOW (before loading members)
    consumer.poll(timeout_ms=10_000)
    log.info("Consumer partition assignment complete — offset position locked")

    if not dry_run:
        ensure_table(bq)
    members = load_members(bq)

    # Buffer: date → msno → [records]
    logs_by_date: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    txns_by_date: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    flushed_dates: set[str] = set()
    # Track EOD receipt per topic — flush only when both are received
    eod_logs: set[str] = set()
    eod_txns: set[str] = set()
    current_date: str | None = None

    # Background worker: runs BQ write + Feast materialize serially so the
    # consumer loop is never blocked waiting for slow storage operations.
    _bg_queue: queue.Queue = queue.Queue()

    def _bg_worker() -> None:
        while True:
            item = _bg_queue.get()
            if item is None:
                break
            date_str, rows = item
            write_to_bq(bq, rows, dry_run)
            materialize(date_str, dry_run)
            _bg_queue.task_done()

    _bg_thread = threading.Thread(target=_bg_worker, daemon=True)
    _bg_thread.start()

    def flush(date: str) -> None:
        if date in flushed_dates:
            return
        all_msno = list(set(logs_by_date[date]) | set(txns_by_date[date]))
        log.info("Flushing date %s — logs: %d users, txns: %d users",
                 date, len(logs_by_date[date]), len(txns_by_date[date]))
        if notify_url:
            _notify(notify_url, date, all_msno)
        flushed_dates.add(date)
        rows, updates = build_rows(date, logs_by_date[date], txns_by_date[date], members)
        # Update in-memory baseline so next day accumulates on top of today's cumulative
        members.update(updates)
        del logs_by_date[date]
        del txns_by_date[date]
        _bg_queue.put((date, rows))

    try:
        for msg in consumer:
            record = msg.value
            topic  = msg.topic

            # End-of-day marker sent by producer on BOTH topics after each day.
            # Only flush when both markers are received — prevents flushing before
            # all transactions are consumed (two topics are read interleaved).
            if record.get("_end_of_day"):
                eod_date = record.get("date", "")[:10]
                if eod_date:
                    if topic == TOPIC_USER_LOGS:
                        eod_logs.add(eod_date)
                    else:
                        eod_txns.add(eod_date)
                    if eod_date in eod_logs and eod_date in eod_txns:
                        if eod_date not in flushed_dates:
                            flush(eod_date)
                current_date = None
                continue

            if topic == TOPIC_USER_LOGS:
                date = record.get("date", "")[:10]
                msno = record.get("msno")
                if date and msno:
                    logs_by_date[date][msno].append(record)
                    current_date = date
            elif topic == TOPIC_TRANSACTIONS:
                date = record.get("transaction_date", "")[:10]
                msno = record.get("msno")
                if date and msno:
                    txns_by_date[date][msno].append(record)
                    current_date = date
            else:
                continue

    except Exception as e:
        log.error("Consumer error: %s", e)
        raise
    finally:
        # Flush any dates that never received both EOD markers (e.g. on shutdown)
        remaining = sorted(
            (set(logs_by_date) | set(txns_by_date)) - flushed_dates
        )
        for d in remaining:
            log.warning("Flushing date %s on shutdown (EOD markers may not have arrived)", d)
            flush(d)
        # Wait for background BQ/Feast work to finish, then stop worker
        _bg_queue.join()
        _bg_queue.put(None)
        _bg_thread.join()
        consumer.close()
        log.info("Consumer closed")


# ── Entry point ───────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="KKBox Kafka consumer — streaming feature aggregation")
    p.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Consume and aggregate without writing to BigQuery or Redis",
    )
    p.add_argument(
        "--notify-url",
        default=None,
        help="URL to POST after each date is flushed (used by streaming simulation UI)",
    )
    p.add_argument(
        "--group-id",
        default=os.getenv("KAFKA_GROUP_ID", "kkbox-feature-consumer"),
        help="Kafka consumer group ID (default: kkbox-feature-consumer)",
    )
    p.add_argument(
        "--offset-reset",
        default="earliest",
        choices=["earliest", "latest"],
        help="auto_offset_reset policy (default: earliest)",
    )
    p.add_argument(
        "--idle-timeout-ms",
        type=int,
        default=30_000,
        help="Stop consuming after N ms of no messages (default: 30000)",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    log.info("Starting consumer — servers=%s  dry_run=%s  group=%s  offset_reset=%s  idle_timeout=%dms",
             args.bootstrap_servers, args.dry_run, args.group_id, args.offset_reset, args.idle_timeout_ms)
    consume(bootstrap_servers=args.bootstrap_servers, dry_run=args.dry_run,
            notify_url=args.notify_url, group_id=args.group_id,
            auto_offset_reset=args.offset_reset, idle_timeout_ms=args.idle_timeout_ms)
