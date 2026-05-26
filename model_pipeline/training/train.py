"""
XGBoost training script — KKBox Churn Prediction.

Data: BigQuery `kkbox_gold.features_train`
      (customer features computed up to 2016-12-31, historical playback snapshot)
Split: out-of-time on registration_init_time < 2016-06-01
"""
from __future__ import annotations

import argparse
import json
import logging
import os

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
import pandas_gbq
import xgboost as xgb
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    f1_score,
    log_loss,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "kkbox-churn-prediction-493716")
BQ_TABLE = os.getenv(
    "BQ_FEATURES_TABLE",
    "kkbox-churn-prediction-493716.kkbox_gold.features_train",
)
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "mlruns")
MLFLOW_EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT_NAME", "kkbox-churn-xgboost")
REGISTERED_MODEL_NAME = os.getenv("MODEL_NAME", "kkbox-churn-model")

SPLIT_DATE = pd.Timestamp("2016-06-01")

DROP_COLS = ["msno", "registration_init_time", "latest_expire_date"]

NUM_COLS = [
    "total_transactions",
    "total_amount_paid",
    "avg_amount_paid",
    "auto_renew_count",
    "cancel_count",
    "total_log_days",
    "total_secs",
    "avg_daily_secs",
    "total_num_25",
    "total_num_50",
    "total_num_75",
    "total_num_985",
    "total_num_100",
    "total_num_unq",
]

GENDER_MAP = {"male": 0, "female": 1, "unknown": 2}

FEATURE_COLS = [
    "city",
    "bd",
    "gender",
    "registered_via",
    "total_transactions",
    "total_amount_paid",
    "avg_amount_paid",
    "auto_renew_count",
    "cancel_count",
    "total_log_days",
    "total_secs",
    "avg_daily_secs",
    "total_num_25",
    "total_num_50",
    "total_num_75",
    "total_num_985",
    "total_num_100",
    "total_num_unq",
]

TARGET_COL = "is_churn"

DEFAULT_PARAMS: dict = {
    "n_estimators": 500,
    "max_depth": 6,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 10,
    "eval_metric": "auc",
    "early_stopping_rounds": 30,
    "random_state": 42,
}


# ── Data loading ─────────────────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    log.info("Loading data from BigQuery: %s", BQ_TABLE)
    df = pandas_gbq.read_gbq(f"SELECT * FROM `{BQ_TABLE}`", project_id=PROJECT_ID)
    log.info("Loaded shape=%s  churn_rate=%.2f%%", df.shape, df[TARGET_COL].mean() * 100)
    return df


# ── Preprocessing ─────────────────────────────────────────────────────────────

def preprocess(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Return processed DataFrame and the preprocessing config needed for inference."""
    df = df.copy()

    # Compute fill values from training data before dropping anything
    bd_median = float(df["bd"].median())
    city_mode = int(df["city"].mode()[0])
    registered_via_mode = int(df["registered_via"].mode()[0])

    preprocessing_config = {
        "drop_cols": DROP_COLS,
        "num_cols_fill_zero": NUM_COLS,
        "bd_median": bd_median,
        "city_mode": city_mode,
        "registered_via_mode": registered_via_mode,
        "gender_map": GENDER_MAP,
    }

    df[NUM_COLS] = df[NUM_COLS].fillna(0)
    df["gender"] = df["gender"].fillna("unknown")
    df["bd"] = df["bd"].fillna(bd_median)
    df["city"] = df["city"].fillna(city_mode)
    df["registered_via"] = df["registered_via"].fillna(registered_via_mode)
    df["gender"] = df["gender"].map(GENDER_MAP).fillna(2).astype(int)

    remaining_nulls = df[FEATURE_COLS + [TARGET_COL]].isnull().sum().sum()
    log.info("Preprocessing done — remaining nulls: %d", remaining_nulls)

    return df, preprocessing_config


# ── Split ─────────────────────────────────────────────────────────────────────

def split(df: pd.DataFrame, reg_time: pd.Series) -> tuple:
    train_mask = reg_time < SPLIT_DATE
    test_mask = reg_time >= SPLIT_DATE

    X_train = df.loc[train_mask, FEATURE_COLS]
    y_train = df.loc[train_mask, TARGET_COL]
    X_test = df.loc[test_mask, FEATURE_COLS]
    y_test = df.loc[test_mask, TARGET_COL]

    log.info(
        "Train: %d rows (%.2f%% churn)  Test: %d rows (%.2f%% churn)",
        len(X_train), y_train.mean() * 100,
        len(X_test), y_test.mean() * 100,
    )
    return X_train, y_train, X_test, y_test


# ── Training ──────────────────────────────────────────────────────────────────

def train(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    params: dict,
    preprocessing_config: dict,
) -> None:
    scale_pos_weight = float((y_train == 0).sum() / (y_train == 1).sum())
    log.info("scale_pos_weight: %.2f", scale_pos_weight)

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name="xgboost_baseline") as run:
        model = xgb.XGBClassifier(**params, scale_pos_weight=scale_pos_weight)
        model.fit(
            X_train,
            y_train,
            eval_set=[(X_test, y_test)],
            verbose=50,
        )

        y_prob = model.predict_proba(X_test)[:, 1]
        y_pred = (y_prob >= 0.5).astype(int)

        auc_roc = roc_auc_score(y_test, y_prob)
        auc_pr = average_precision_score(y_test, y_prob)
        logloss = log_loss(y_test, y_prob)
        f1 = f1_score(y_test, y_pred)
        prec = precision_score(y_test, y_pred)
        rec = recall_score(y_test, y_pred)

        precisions, recalls, thresholds = precision_recall_curve(y_test, y_prob)
        f1_scores = 2 * precisions * recalls / (precisions + recalls + 1e-9)
        best_idx = int(np.argmax(f1_scores))
        best_threshold = float(thresholds[best_idx])

        log.info("=== Evaluation ===")
        log.info("AUC-ROC:        %.4f", auc_roc)
        log.info("AUC-PR:         %.4f", auc_pr)
        log.info("Log Loss:       %.4f", logloss)
        log.info("F1 (t=0.5):     %.4f", f1)
        log.info("Precision:      %.4f", prec)
        log.info("Recall:         %.4f", rec)
        log.info("Best threshold: %.3f  (F1=%.4f)", best_threshold, f1_scores[best_idx])
        log.info("\n%s", classification_report(y_test, y_pred, target_names=["no churn", "churn"]))

        loggable_params = {k: v for k, v in params.items() if k != "early_stopping_rounds"}
        mlflow.log_params({**loggable_params, "scale_pos_weight": scale_pos_weight})
        mlflow.log_metrics(
            {
                "auc_roc": auc_roc,
                "auc_pr": auc_pr,
                "log_loss": logloss,
                "f1": f1,
                "precision": prec,
                "recall": rec,
                "best_threshold": best_threshold,
            }
        )

        mlflow.log_dict(preprocessing_config, "preprocessing_config.json")
        mlflow.log_dict({"feature_cols": FEATURE_COLS}, "feature_cols.json")
        mlflow.xgboost.log_model(
            model,
            name="model",
            registered_model_name=REGISTERED_MODEL_NAME,
        )

        log.info("MLflow run ID: %s", run.info.run_id)


# ── Entry point ───────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Train XGBoost churn model")
    p.add_argument("--n-estimators", type=int, default=DEFAULT_PARAMS["n_estimators"])
    p.add_argument("--max-depth", type=int, default=DEFAULT_PARAMS["max_depth"])
    p.add_argument("--learning-rate", type=float, default=DEFAULT_PARAMS["learning_rate"])
    p.add_argument("--subsample", type=float, default=DEFAULT_PARAMS["subsample"])
    p.add_argument("--colsample-bytree", type=float, default=DEFAULT_PARAMS["colsample_bytree"])
    p.add_argument("--min-child-weight", type=int, default=DEFAULT_PARAMS["min_child_weight"])
    p.add_argument(
        "--early-stopping-rounds",
        type=int,
        default=DEFAULT_PARAMS["early_stopping_rounds"],
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    params = {
        "n_estimators": args.n_estimators,
        "max_depth": args.max_depth,
        "learning_rate": args.learning_rate,
        "subsample": args.subsample,
        "colsample_bytree": args.colsample_bytree,
        "min_child_weight": args.min_child_weight,
        "eval_metric": "auc",
        "early_stopping_rounds": args.early_stopping_rounds,
        "random_state": 42,
    }

    df = load_data()
    reg_time = pd.to_datetime(df["registration_init_time"])
    df_proc, preprocessing_config = preprocess(df)
    X_train, y_train, X_test, y_test = split(df_proc, reg_time)
    train(X_train, y_train, X_test, y_test, params, preprocessing_config)


if __name__ == "__main__":
    main()
