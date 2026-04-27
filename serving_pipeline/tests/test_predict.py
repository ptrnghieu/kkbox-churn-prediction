"""Tests for prediction endpoints and Prometheus metrics."""
from pathlib import Path
import sys

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.main import app
from app.predict import get_prediction_service


class FakePredictionService:
    def predict_single(self, msno: str) -> dict:
        return {
            "msno": msno,
            "churn_probability": 0.9,
            "is_churn": 1,
        }

    def predict_batch(self, msnos: list[str]) -> list[dict]:
        return [
            {
                "msno": msno,
                "churn_probability": 0.1 if index == 0 else 0.8,
                "is_churn": 0 if index == 0 else 1,
            }
            for index, msno in enumerate(msnos)
        ]


client = TestClient(app)


def setup_module():
    app.dependency_overrides[get_prediction_service] = lambda: FakePredictionService()


def teardown_module():
    app.dependency_overrides.clear()


def test_health_check():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_single_prediction():
    response = client.post("/predict/", json={"msno": "user_1"})

    assert response.status_code == 200
    assert response.json()["msno"] == "user_1"
    assert response.json()["is_churn"] == 1


def test_batch_prediction():
    response = client.post(
        "/predict/batch",
        json=[{"msno": "user_1"}, {"msno": "user_2"}],
    )

    assert response.status_code == 200
    assert len(response.json()) == 2


def test_metrics_endpoint_exposes_serving_metrics():
    client.get("/health")
    client.post("/predict/", json={"msno": "metrics_user"})
    client.post("/predict/batch", json=[{"msno": "user_1"}, {"msno": "user_2"}])

    response = client.get("/metrics")

    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    body = response.text
    assert "serving_http_requests_total" in body
    assert 'serving_prediction_requests_total{endpoint="/predict/",kind="single"}' in body
    assert 'serving_prediction_requests_total{endpoint="/predict/batch",kind="batch"}' in body
    assert "serving_batch_prediction_size_bucket" in body
