"""Simple in-memory prediction statistics counter."""
import threading

_lock = threading.Lock()
_stats = {"total": 0, "churn": 0, "retain": 0}


def record(is_churn: int) -> None:
    with _lock:
        _stats["total"] += 1
        if is_churn:
            _stats["churn"] += 1
        else:
            _stats["retain"] += 1


def get() -> dict:
    with _lock:
        t, c, r = _stats["total"], _stats["churn"], _stats["retain"]
    return {
        "total_predictions": t,
        "churn_count": c,
        "retain_count": r,
        "churn_rate": round(c / t, 4) if t > 0 else 0.0,
    }
