from feast import FeatureView, Field
from feast.types import Int64, Float32, String
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from datetime import timedelta
from dotenv import load_dotenv
import os

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
TABLE_NAME = os.getenv("TABLE_NAME", "features")

feature_source = BigQuerySource(
    table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME}",
    timestamp_field="event_timestamp"        # table bigquery phai co, fix sau
)

# 3. FeatureView
kkbox_fv = FeatureView(
    name="kkbox_features",
    entities=["msno"],
    ttl=timedelta(days=30),
    schema=[
        # member
        Field(name="city", dtype=Int64),
        Field(name="bd", dtype=Int64),
        Field(name="gender", dtype=String),
        Field(name="registered_via", dtype=Int64),

        #transaction
        Field(name="total_transactions", dtype=Int64),
        Field(name="total_amount_paid", dtype=Float32),
        Field(name="avg_amount_paid", dtype=Float32),
        Field(name="auto_renew_count", dtype=Int64),
        Field(name="cancel_count", dtype=Int64),

        #log
        Field(name="total_log_days", dtype=Int64),
        Field(name="total_secs", dtype=Float32),
        Field(name="avg_daily_secs", dtype=Float32),
        Field(name="total_num_25", dtype=Int64),
        Field(name="total_num_50", dtype=Int64),
        Field(name="total_num_75", dtype=Int64),
        Field(name="total_num_985", dtype=Int64),
        Field(name="total_num_100", dtype=Int64),
        Field(name="total_num_unq", dtype=Int64),
    ],
    source=feature_source
)