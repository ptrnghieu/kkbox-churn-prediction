"""Prometheus metrics for the serving API."""
from time import perf_counter

from prometheus_client import Counter, Gauge, Histogram

HTTP_REQUESTS_TOTAL = Counter(
    "serving_http_requests_total",
    "Total number of HTTP requests handled by the serving API.",
    ["method", "path", "status_code"],
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "serving_http_request_duration_seconds",
    "Latency of HTTP requests handled by the serving API.",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

HTTP_REQUESTS_IN_PROGRESS = Gauge(
    "serving_http_requests_in_progress",
    "Number of in-flight HTTP requests.",
    ["method", "path"],
)

PREDICTION_REQUESTS_TOTAL = Counter(
    "serving_prediction_requests_total",
    "Total number of prediction requests.",
    ["endpoint", "kind"],
)

PREDICTION_RESULTS_TOTAL = Counter(
    "serving_prediction_results_total",
    "Total number of prediction results by churn class.",
    ["endpoint", "is_churn"],
)

PREDICTION_CHURN_PROBABILITY = Histogram(
    "serving_prediction_churn_probability",
    "Distribution of churn probability returned by the model.",
    ["endpoint"],
    buckets=(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0),
)

BATCH_PREDICTION_SIZE = Histogram(
    "serving_batch_prediction_size",
    "Number of records submitted to batch prediction.",
    buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000),
)

FEAST_ONLINE_FETCH_TOTAL = Counter(
    "serving_feast_online_fetch_total",
    "Total number of Feast online feature fetch attempts.",
    ["status"],
)

FEAST_ONLINE_FETCH_DURATION_SECONDS = Histogram(
    "serving_feast_online_fetch_duration_seconds",
    "Latency of Feast online feature fetch calls.",
    ["status"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)


def observe_prediction_result(endpoint: str, churn_probability: float, is_churn: int) -> None:
    """Record result metrics for a prediction response."""
    PREDICTION_RESULTS_TOTAL.labels(endpoint=endpoint, is_churn=str(is_churn)).inc()
    PREDICTION_CHURN_PROBABILITY.labels(endpoint=endpoint).observe(churn_probability)


class FeastFetchTimer:
    """Context manager for Feast online fetch metrics."""

    def __enter__(self):
        self.started_at = perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        status = "error" if exc_type else "success"
        FEAST_ONLINE_FETCH_TOTAL.labels(status=status).inc()
        FEAST_ONLINE_FETCH_DURATION_SECONDS.labels(status=status).observe(
            perf_counter() - self.started_at
        )
        return False
