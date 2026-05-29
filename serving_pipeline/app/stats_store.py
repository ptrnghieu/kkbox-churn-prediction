"""Simple in-memory prediction statistics and history store."""
import threading
from collections import deque

_MAX_HISTORY = 2000  # cap to avoid unbounded memory growth

_lock = threading.Lock()
_stats = {"total": 0, "churn": 0, "retain": 0}
_churn_history: deque[dict] = deque(maxlen=_MAX_HISTORY)


def record(msno: str, churn_probability: float, is_churn: int) -> None:
    with _lock:
        _stats["total"] += 1
        if is_churn:
            _stats["churn"] += 1
            _churn_history.append({"msno": msno, "churn_probability": churn_probability})
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


def get_churned(limit: int = 500) -> list[dict]:
    with _lock:
        items = list(_churn_history)
    items.sort(key=lambda x: x["churn_probability"], reverse=True)
    return items[:limit]
