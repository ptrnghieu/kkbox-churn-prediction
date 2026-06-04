"""Feature drift detection — PSI + KS test per feature per streaming day.

Reference distribution is loaded once (lazy) from BigQuery features_train.
After each /stream/notify the stream router schedules run_drift_check(date)
~90 seconds later (allowing the background BQ write to finish first).
"""
from __future__ import annotations

import asyncio
import logging
import os

import numpy as np
from fastapi import APIRouter
from prometheus_client import Gauge

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Drift"])

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "kkbox-churn-prediction-493716")

NUMERICAL_FEATURES = [
    "total_transactions", "total_amount_paid", "avg_amount_paid",
    "auto_renew_count", "cancel_count", "total_log_days",
    "total_secs", "avg_daily_secs",
    "total_num_25", "total_num_50", "total_num_75",
    "total_num_985", "total_num_100", "total_num_unq",
]

N_BINS = 10

# ── In-memory state ────────────────────────────────────────────────────────────
_reference: dict = {}                  # feat → {bins, counts, values}
_drift_results: dict[str, dict] = {}   # date → {psi: {feat: float}, ks: {feat: {stat,pvalue}}}

# ── Prometheus ─────────────────────────────────────────────────────────────────
_DRIFT_PSI = Gauge(
    "kkbox_feature_psi",
    "Population Stability Index per feature per streaming day",
    ["feature", "date"],
)
_DRIFT_KS = Gauge(
    "kkbox_feature_ks_stat",
    "KS test statistic per feature per streaming day",
    ["feature", "date"],
)


# ── Reference loading ──────────────────────────────────────────────────────────

def _load_reference_sync() -> None:
    import pandas_gbq
    global _reference
    if _reference:
        return
    logger.info("Loading reference feature distributions from BigQuery features_train ...")
    cols = ", ".join(NUMERICAL_FEATURES)
    df = pandas_gbq.read_gbq(
        f"SELECT {cols} FROM `{GCP_PROJECT_ID}.kkbox_gold.features_train`",
        project_id=GCP_PROJECT_ID,
    )
    for feat in NUMERICAL_FEATURES:
        vals = df[feat].dropna().values.astype(float)
        if len(vals) == 0:
            continue
        counts, bins = np.histogram(vals, bins=N_BINS)
        _reference[feat] = {"bins": bins, "counts": counts, "values": vals}
    logger.info("Reference distributions loaded for %d features", len(_reference))


# ── Metrics ────────────────────────────────────────────────────────────────────

def _psi(ref_counts: np.ndarray, cur_counts: np.ndarray) -> float:
    eps = 1e-6
    ref_pct = ref_counts / (ref_counts.sum() + eps)
    cur_pct = cur_counts / (cur_counts.sum() + eps)
    return float(np.sum(
        (np.clip(cur_pct, eps, None) - np.clip(ref_pct, eps, None))
        * np.log(np.clip(cur_pct, eps, None) / np.clip(ref_pct, eps, None))
    ))


def _ks(ref_vals: np.ndarray, cur_vals: np.ndarray) -> tuple[float, float]:
    from scipy import stats
    # Cap reference sample at 5 000 rows for speed
    if len(ref_vals) > 5_000:
        ref_vals = np.random.default_rng(42).choice(ref_vals, 5_000, replace=False)
    stat, pvalue = stats.ks_2samp(ref_vals, cur_vals)
    return float(stat), float(pvalue)


def _level(psi: float) -> str:
    if psi < 0.10:
        return "stable"
    if psi < 0.25:
        return "warning"
    return "critical"


# ── Core drift computation ─────────────────────────────────────────────────────

def _run_drift_sync(date_str: str) -> None:
    import pandas_gbq
    if not _reference:
        _load_reference_sync()

    logger.info("Running drift check for %s ...", date_str)
    cols = ", ".join(NUMERICAL_FEATURES)
    try:
        df = pandas_gbq.read_gbq(
            f"SELECT {cols} FROM `{GCP_PROJECT_ID}.kkbox_gold.features_streaming`"
            f" WHERE DATE(event_timestamp) = '{date_str}'",
            project_id=GCP_PROJECT_ID,
        )
    except Exception as exc:
        logger.error("Drift BQ query failed for %s: %s", date_str, exc)
        return

    if df.empty:
        logger.warning("No rows in features_streaming for %s — skipping drift", date_str)
        return

    psi_scores: dict[str, float] = {}
    ks_scores: dict[str, dict] = {}

    for feat in NUMERICAL_FEATURES:
        ref = _reference.get(feat)
        if ref is None:
            continue
        cur_vals = df[feat].dropna().values.astype(float)
        if len(cur_vals) == 0:
            continue

        cur_counts, _ = np.histogram(cur_vals, bins=ref["bins"])
        psi = _psi(ref["counts"], cur_counts)
        psi_scores[feat] = round(psi, 4)
        _DRIFT_PSI.labels(feature=feat, date=date_str).set(psi)

        ks_s, ks_p = _ks(ref["values"], cur_vals)
        ks_scores[feat] = {"stat": round(ks_s, 4), "pvalue": round(ks_p, 6)}
        _DRIFT_KS.labels(feature=feat, date=date_str).set(ks_s)

    _drift_results[date_str] = {"psi": psi_scores, "ks": ks_scores}
    max_psi = max(psi_scores.values()) if psi_scores else 0
    logger.info(
        "Drift check complete for %s — max_PSI=%.4f  features=%d",
        date_str, max_psi, len(psi_scores),
    )


async def run_drift_check(date_str: str) -> None:
    """Schedule drift computation off the event loop (BQ I/O is blocking)."""
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, _run_drift_sync, date_str)
    except Exception as exc:
        logger.error("Drift check failed for %s: %s", date_str, exc)


def clear() -> None:
    """Reset drift results — called when streaming simulation is stopped/restarted."""
    _drift_results.clear()
    logger.info("Drift results cleared")


# ── API endpoints ──────────────────────────────────────────────────────────────

@router.get("/drift")
def get_all_drift() -> dict:
    """Summary PSI status for all processed dates."""
    summary: dict[str, dict] = {}
    for date, res in _drift_results.items():
        psi = res.get("psi", {})
        vals = list(psi.values())
        max_psi = max(vals) if vals else None
        summary[date] = {
            "max_psi": round(max_psi, 4) if max_psi is not None else None,
            "level": _level(max_psi) if max_psi is not None else "pending",
            "n_warning": sum(1 for v in vals if 0.10 <= v < 0.25),
            "n_critical": sum(1 for v in vals if v >= 0.25),
            "n_features": len(vals),
        }
    return {"dates": summary}


@router.get("/drift/{date}")
def get_drift_date(date: str) -> dict:
    """Per-feature PSI + KS results for one streaming date."""
    res = _drift_results.get(date)
    if res is None:
        return {"date": date, "status": "pending", "features": {}}

    psi = res.get("psi", {})
    ks = res.get("ks", {})
    features: dict[str, dict] = {}
    for feat in NUMERICAL_FEATURES:
        psi_val = psi.get(feat)
        if psi_val is None:
            continue
        ks_info = ks.get(feat, {})
        features[feat] = {
            "psi": psi_val,
            "level": _level(psi_val),
            "ks_stat": ks_info.get("stat"),
            "ks_pvalue": ks_info.get("pvalue"),
            "ks_drift": (ks_info.get("pvalue") or 1.0) < 0.05,
        }
    return {"date": date, "status": "done", "features": features}
