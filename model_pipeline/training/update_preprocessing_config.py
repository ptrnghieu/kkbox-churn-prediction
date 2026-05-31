"""
One-off script: regenerate preprocessing_config.json with num_cols_medians
and upload to GCS. Run after updating train.py — no retraining needed.
"""
import json
import os
import pandas as pd
from google.cloud import bigquery, storage

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "kkbox-churn-prediction-493716")
GCS_BUCKET     = os.getenv("GCS_BUCKET",     "kkbox-churn-prediction-493716-data")
GCS_PREFIX     = os.getenv("GCS_MODEL_PREFIX","models/kkbox-churn-xgboost")

NUM_COLS = [
    "total_transactions","total_amount_paid","avg_amount_paid",
    "auto_renew_count","cancel_count","total_log_days","total_secs",
    "avg_daily_secs","total_num_25","total_num_50","total_num_75",
    "total_num_985","total_num_100","total_num_unq",
]
GENDER_MAP = {"male": 0, "female": 1, "unknown": 2}

print("Fetching existing config from GCS...")
client = storage.Client(project=GCP_PROJECT_ID)
blob = client.bucket(GCS_BUCKET).blob(f"{GCS_PREFIX}/preprocessing_config.json")
config = json.loads(blob.download_as_text())

print("Computing medians from features_train in BigQuery...")
bq = bigquery.Client(project=GCP_PROJECT_ID)
cols = ", ".join(NUM_COLS + ["bd", "city", "registered_via"])
query = f"SELECT {cols} FROM `{GCP_PROJECT_ID}.kkbox_gold.features_train`"
df = bq.query(query).to_dataframe()

config["num_cols_medians"] = {col: float(df[col].median()) for col in NUM_COLS}
config["bd_median"]             = float(df["bd"].median())
config["city_mode"]             = int(df["city"].mode()[0])
config["registered_via_mode"]  = int(df["registered_via"].mode()[0])

print("Medians:", config["num_cols_medians"])

blob.upload_from_string(json.dumps(config))
print(f"Uploaded updated preprocessing_config.json to gs://{GCS_BUCKET}/{GCS_PREFIX}/")
