.PHONY: help setup lint test start stop logs spark-bronze-silver spark-silver-gold

help:
	@echo "KKBox Churn Prediction — Available Commands"
	@echo "============================================"
	@echo "  make setup               Install dependencies"
	@echo "  make lint                Run black + ruff"
	@echo "  make test                Run all tests with coverage"
	@echo "  make start               Start all Docker services"
	@echo "  make stop                Stop all Docker services"
	@echo "  make logs                Tail Docker logs"
	@echo "  make spark-bronze-silver Submit Bronze→Silver Spark job"
	@echo "  make spark-silver-gold   Submit Silver→Gold Spark job"

setup:
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -e ".[dev]"
	cp -n .env.example .env || true
	@echo "✅ Setup done."
	@echo "👉 Activate: source .venv/bin/activate"

lint:
	black .
	ruff check . --fix

test:
	pytest --cov=. --cov-report=term-missing -v

start:
	docker-compose -f infra/docker-compose.yml up -d

stop:
	docker-compose -f infra/docker-compose.yml down

logs:
	docker-compose -f infra/docker-compose.yml logs -f

spark-bronze-silver:
	@echo "Submitting Bronze→Silver Spark job..."
	gcloud dataproc jobs submit pyspark \
		gs://kkbox-churn-prediction-493716-data/scripts/bronze_to_silver.py \
		--cluster=kkbox-spark-cluster \
		--region=asia-southeast1
	@echo "⚠️  Remember to delete the cluster after job finishes!"

spark-silver-gold:
	@echo "Submitting Silver→Gold Spark job..."
	gcloud dataproc jobs submit pyspark \
		gs://kkbox-churn-prediction-493716-data/scripts/silver_to_gold.py \
		--cluster=kkbox-spark-cluster \
		--region=asia-southeast1
	@echo "⚠️  Remember to delete the cluster after job finishes!"
