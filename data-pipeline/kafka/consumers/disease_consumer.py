"""
Disease pipeline consumer — reads from all 4 source topics and:
  1. Appends new records to the bronze parquet on MinIO
  2. Re-runs the Silver→Gold transform for that source
  3. Updates the Feast online store

Run from data-pipeline/:
    python -m kafka.consumers.disease_consumer
"""
import io
import json
import subprocess
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone

from kafka.config import TOPICS, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_RAW
from kafka.consumers.base_consumer import BaseConsumer

# Map source name → (bronze MinIO key, transform module)
SOURCE_CONFIG = {
    "ilinet":    ("bronze/ilinet.parquet",    "transforms.ilinet_transform"),
    "covidcast": ("bronze/covidcast.parquet", "transforms.covidcast_transform"),
    "flusurv":   ("bronze/flusurv.parquet",   "transforms.flusurv_transform"),
    "noaa":      ("bronze/noaa.parquet",       "transforms.noaa_transform"),
}


def _get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def _append_to_minio(s3, bucket: str, key: str, new_df: pd.DataFrame) -> None:
    """
    Append new_df to the existing parquet file at s3://bucket/key.
    If the file does not exist yet, creates it.
    Deduplicates on all columns to avoid duplicates from reprocessed messages.
    """
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        existing_df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        combined = pd.concat([existing_df, new_df], ignore_index=True).drop_duplicates()
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            combined = new_df
        else:
            raise

    buf = io.BytesIO()
    combined.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    print(f"[consumer] Wrote {len(combined)} rows ({len(new_df)} new) → s3://{bucket}/{key}")


def _run_transform(source: str) -> None:
    """
    Run the Silver→Gold transform for the given source as a subprocess.
    This re-generates the gold parquet and updates the Feast registry.
    """
    module_map = {
        "ilinet":    "transforms.ilinet_transform",
        "covidcast": "transforms.covidcast_transform",
        "flusurv":   "transforms.flusurv_transform",
        "noaa":      "transforms.noaa_transform",
    }
    module = module_map[source]
    print(f"[consumer] Running transform: {module}")
    result = subprocess.run(
        ["python", "-m", module],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Transform failed for {source}:\n{result.stderr}")
    print(result.stdout.strip())


def _update_feast() -> None:
    """Run feast materialize-incremental to push new gold features to Redis."""
    end_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    print(f"[consumer] Running feast materialize-incremental {end_ts}")
    result = subprocess.run(
        ["feast", "-c", "feature_repo", "materialize-incremental", end_ts],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"[consumer] feast warning: {result.stderr.strip()}")
    else:
        print(result.stdout.strip())


class DiseaseConsumer(BaseConsumer):

    def __init__(self):
        super().__init__(topics=list(TOPICS.values()))
        self.s3 = _get_s3_client()

    def handle_message(self, source: str, records: list[dict], published_at: str) -> None:
        if source not in SOURCE_CONFIG:
            print(f"[consumer] Unknown source '{source}', skipping.")
            return

        minio_key, _ = SOURCE_CONFIG[source]

        # 1. Convert records to DataFrame
        new_df = pd.DataFrame(records)
        new_df["kafka_published_at"] = published_at

        # 2. Append to MinIO bronze parquet
        _append_to_minio(self.s3, MINIO_BUCKET_RAW, minio_key, new_df)

        # 3. Re-run Silver→Gold transform
        _run_transform(source)

        # 4. Update Feast online store
        _update_feast()

        print(f"[consumer] Pipeline complete for source '{source}'.")


if __name__ == "__main__":
    consumer = DiseaseConsumer()
    consumer.run()
