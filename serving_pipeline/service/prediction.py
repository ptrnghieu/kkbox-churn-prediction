import json
import logging
import os
import tempfile

import numpy as np
import pandas as pd
import shap
import xgboost as xgb
from app.metrics import FeastFetchTimer
from feast import FeatureStore
from google.cloud import storage as gcs

logger = logging.getLogger(__name__)

GCS_BUCKET = os.getenv("GCS_BUCKET", "kkbox-churn-prediction-493716-data")
GCS_MODEL_PREFIX = os.getenv("GCS_MODEL_PREFIX", "models/kkbox-churn-xgboost")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "kkbox-churn-prediction-493716")
CHURN_THRESHOLD = float(os.getenv("CHURN_THRESHOLD", "0.781"))

# Human-readable reason for each feature when it contributes to churn risk
CHURN_REASONS: dict[str, str] = {
    "cancel_count":       "Khách hàng đã hủy gói nhiều lần",
    "total_log_days":     "Khách hàng ít hoạt động trên nền tảng",
    "total_secs":         "Tổng thời gian nghe nhạc thấp",
    "avg_daily_secs":     "Thời gian nghe nhạc hàng ngày thấp",
    "auto_renew_count":   "Khách hàng ít tự động gia hạn gói",
    "total_transactions": "Ít giao dịch thanh toán được thực hiện",
    "total_amount_paid":  "Tổng số tiền đã thanh toán thấp",
    "avg_amount_paid":    "Giá trị giao dịch trung bình thấp",
    "total_num_unq":      "Ít bài hát khác nhau được nghe",
    "total_num_100":      "Ít bài hát được nghe hoàn toàn",
    "total_num_25":       "Phần lớn bài hát bị bỏ ngang từ đầu",
    "total_num_50":       "Nhiều bài hát bị bỏ giữa chừng",
    "total_num_75":       "Nhiều bài hát không nghe đến cuối",
    "total_num_985":      "Ít bài hát được nghe gần hết",
    "city":               "Khu vực địa lý có tỉ lệ churn cao",
    "registered_via":     "Kênh đăng ký có tỉ lệ churn cao",
    "bd":                 "Nhóm tuổi có tỉ lệ churn cao",
    "gender":             "Nhóm giới tính có tỉ lệ churn cao",
}

# Warnings for retain users — same features but framed as future risks
RETAIN_WARNINGS: dict[str, str] = {
    "cancel_count":       "Số lần hủy gói đang tăng — cần theo dõi",
    "total_log_days":     "Mức độ hoạt động đang có dấu hiệu giảm",
    "total_secs":         "Thời gian nghe nhạc đang giảm đáng kể",
    "avg_daily_secs":     "Thời gian nghe nhạc hàng ngày đang giảm",
    "auto_renew_count":   "Tỉ lệ tự động gia hạn đang giảm",
    "total_transactions": "Số lần giao dịch đang giảm",
    "total_amount_paid":  "Giá trị thanh toán tổng đang giảm",
    "avg_amount_paid":    "Giá trị mỗi giao dịch đang giảm",
    "total_num_unq":      "Đa dạng bài hát đang giảm",
    "total_num_100":      "Ít nghe hết bài hơn trước",
    "total_num_25":       "Xu hướng bỏ bài từ đầu đang tăng",
    "total_num_50":       "Xu hướng bỏ bài giữa chừng đang tăng",
    "total_num_75":       "Ít nghe đến cuối bài hơn trước",
    "total_num_985":      "Mức độ tương tác với bài hát đang giảm",
    "city":               "Khu vực có xu hướng churn tăng gần đây",
    "registered_via":     "Kênh đăng ký có dấu hiệu churn tăng",
    "bd":                 "Nhóm tuổi đang có tỉ lệ rời bỏ cao hơn",
    "gender":             "Nhóm giới tính đang có tỉ lệ rời bỏ cao hơn",
}


def _load_artifacts() -> tuple[xgb.XGBClassifier, dict, list[str]]:
    logger.info("Loading model artifacts from gs://%s/%s", GCS_BUCKET, GCS_MODEL_PREFIX)
    client = gcs.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)

    with tempfile.NamedTemporaryFile(suffix=".ubj", delete=False) as tmp:
        bucket.blob(f"{GCS_MODEL_PREFIX}/model.ubj").download_to_filename(tmp.name)
        model = xgb.XGBClassifier()
        model.load_model(tmp.name)

    preprocessing_config = json.loads(
        bucket.blob(f"{GCS_MODEL_PREFIX}/preprocessing_config.json").download_as_text()
    )
    feature_cols = json.loads(
        bucket.blob(f"{GCS_MODEL_PREFIX}/feature_cols.json").download_as_text()
    )["feature_cols"]

    logger.info("Model artifacts loaded. Features: %d", len(feature_cols))
    return model, preprocessing_config, feature_cols


def _preprocess(features: dict, config: dict, feature_cols: list[str]) -> pd.DataFrame:
    row = {}
    for col in feature_cols:
        val = features.get(col)
        if col in config["num_cols_fill_zero"]:
            row[col] = val if val is not None else 0
        elif col == "bd":
            row[col] = val if val is not None else config["bd_median"]
        elif col == "city":
            row[col] = val if val is not None else config["city_mode"]
        elif col == "registered_via":
            row[col] = val if val is not None else config["registered_via_mode"]
        elif col == "gender":
            gender_str = val if val is not None else "unknown"
            row[col] = config["gender_map"].get(gender_str, 2)
        else:
            row[col] = val if val is not None else 0
    return pd.DataFrame([row], columns=feature_cols)


class PredictionService:
    def __init__(self):
        repo_path = os.getenv("FEAST_REPO_PATH")
        if not repo_path:
            repo_path = "/app/feature_store" if os.path.exists("/app/feature_store") else "../feature_store"

        self.store = FeatureStore(repo_path=repo_path)
        self.feature_refs = [
            "kkbox_features:city",
            "kkbox_features:bd",
            "kkbox_features:gender",
            "kkbox_features:registered_via",
            "kkbox_features:total_transactions",
            "kkbox_features:total_amount_paid",
            "kkbox_features:avg_amount_paid",
            "kkbox_features:auto_renew_count",
            "kkbox_features:cancel_count",
            "kkbox_features:total_log_days",
            "kkbox_features:total_secs",
            "kkbox_features:avg_daily_secs",
            "kkbox_features:total_num_25",
            "kkbox_features:total_num_50",
            "kkbox_features:total_num_75",
            "kkbox_features:total_num_985",
            "kkbox_features:total_num_100",
            "kkbox_features:total_num_unq",
        ]

        self.model, self.preprocessing_config, self.feature_cols = _load_artifacts()
        # TreeExplainer is initialized once and reused — fast for XGBoost
        self.explainer = shap.TreeExplainer(self.model)
        logger.info("SHAP TreeExplainer initialized.")

    def _get_features(self, msno: str) -> dict:
        try:
            with FeastFetchTimer():
                response = self.store.get_online_features(
                    features=self.feature_refs,
                    entity_rows=[{"msno": msno}],
                )
            return {k: v[0] for k, v in response.to_dict().items()}
        except Exception:
            logger.exception("Failed to fetch online features for msno=%s", msno)
            return {}

    def _predict(self, X: pd.DataFrame) -> float:
        return float(self.model.predict_proba(X)[0, 1])

    def _top_reasons(self, X: pd.DataFrame, is_churn: int, top_n: int = 3) -> list[str]:
        """Return top_n human-readable reasons based on SHAP values."""
        shap_values = self.explainer.shap_values(X)[0]  # shape: (n_features,)
        # Positive SHAP = pushes toward churn
        feature_shap = list(zip(self.feature_cols, shap_values))
        feature_shap.sort(key=lambda x: x[1], reverse=True)

        reason_map = CHURN_REASONS if is_churn else RETAIN_WARNINGS
        reasons = []
        for feature, shap_val in feature_shap:
            if shap_val <= 0:
                break  # no more positive-SHAP features
            reason = reason_map.get(feature)
            if reason:
                reasons.append(reason)
            if len(reasons) >= top_n:
                break
        return reasons

    def predict_single(self, msno: str) -> dict:
        from app.feature_cache import get_date as _cache_get_date
        features = self._get_features(msno)
        feature_values = {k: v for k, v in features.items() if k != "msno"}
        member_found = bool(feature_values) and any(v is not None for v in feature_values.values())

        X = _preprocess(features, self.preprocessing_config, self.feature_cols)
        churn_prob = self._predict(X)
        is_churn = int(churn_prob >= CHURN_THRESHOLD)
        reasons = self._top_reasons(X, is_churn)

        return {
            "msno": msno,
            "churn_probability": churn_prob,
            "is_churn": is_churn,
            "member_found": member_found,
            "reasons": reasons,
            "feature_timestamp": _cache_get_date(msno),
        }

    def _predict_single_fast(self, msno: str) -> dict:
        """Like predict_single but skips SHAP — used by batch to save time."""
        features = self._get_features(msno)
        feature_values = {k: v for k, v in features.items() if k != "msno"}
        member_found = bool(feature_values) and any(v is not None for v in feature_values.values())
        X = _preprocess(features, self.preprocessing_config, self.feature_cols)
        churn_prob = self._predict(X)
        is_churn = int(churn_prob >= CHURN_THRESHOLD)
        return {
            "msno": msno,
            "churn_probability": churn_prob,
            "is_churn": is_churn,
            "member_found": member_found,
            "reasons": [],
        }

    def predict_batch(self, msnos: list[str]) -> list[dict]:
        return [self._predict_single_fast(msno) for msno in msnos]
