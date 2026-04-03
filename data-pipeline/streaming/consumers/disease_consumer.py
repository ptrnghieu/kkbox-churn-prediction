"""
Disease pipeline consumer — reads from all 4 source topics and:
  1. Appends new records to the local bronze parquet (for the transform to read)
  2. Uploads the updated bronze parquet to MinIO (durable storage)
  3. Re-runs the Silver→Gold transform for that source
  4. Updates the Feast online store

Run from data-pipeline/:
    python -m streaming.consumers.disease_consumer
"""
import io
import os
import subprocess
import pandas as pd
import boto3
from datetime import datetime, timezone

from streaming.config import TOPICS, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_RAW
from streaming.consumers.base_consumer import BaseConsumer

# Map source name → (local bronze path, MinIO key)
SOURCE_CONFIG = {
    "ilinet":    ("data/raw/ilinet.parquet",    "bronze/ilinet.parquet"),
    "covidcast": ("data/raw/covidcast.parquet", "bronze/covidcast.parquet"),
    "flusurv":   ("data/raw/flusurv.parquet",   "bronze/flusurv.parquet"),
    "noaa":      ("data/raw/noaa.parquet",       "bronze/noaa.parquet"),
}

TRANSFORM_MODULE = {
    "ilinet":    "transforms.ilinet_transform",
    "covidcast": "transforms.covidcast_transform",
    "flusurv":   "transforms.flusurv_transform",
    "noaa":      "transforms.noaa_transform",
}


def _get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def _append_to_local(local_path: str, new_df: pd.DataFrame) -> pd.DataFrame:
    """
    Append new_df to the existing local bronze parquet.
    If the file doesn't exist, creates it.
    Deduplicates to avoid reprocessing duplicates.
    """
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    if os.path.exists(local_path):
        try:
            existing_df = pd.read_parquet(local_path)
            combined = pd.concat([existing_df, new_df], ignore_index=True).drop_duplicates()
        except Exception:
            # File corrupted — overwrite with new data
            print(f"[consumer] Warning: {local_path} is corrupted, overwriting.")
            combined = new_df
    else:
        combined = new_df

    combined.to_parquet(local_path, index=False)
    print(f"[consumer] Wrote {len(combined)} rows ({len(new_df)} new) → {local_path}")
    return combined


def _upload_to_minio(s3, combined_df: pd.DataFrame, bucket: str, key: str) -> None:
    """Upload the full bronze DataFrame to MinIO for durable storage."""
    buf = io.BytesIO()
    combined_df.to_parquet(buf, index=False)
    buf.seek(0)
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
        print(f"[consumer] Uploaded {len(combined_df)} rows → s3://{bucket}/{key}")
    except Exception as e:
        print(f"[consumer] MinIO upload warning (non-fatal): {e}")


_DATA_PIPELINE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _run_transform(source: str) -> None:
    """Re-run Silver→Gold transform for the given source."""
    module = TRANSFORM_MODULE[source]
    print(f"[consumer] Running transform: {module}")
    result = subprocess.run(
        ["python", "-m", module],
        capture_output=True, text=True,
        cwd=_DATA_PIPELINE_DIR,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Transform failed for {source}:\n{result.stderr}")
    print(result.stdout.strip())


def _update_feast() -> None:
    """Push new gold features to Redis online store."""
    end_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    print(f"[consumer] Running feast materialize-incremental {end_ts}")
    result = subprocess.run(
        ["feast", "-c", "feature_repo", "materialize-incremental", end_ts],
        capture_output=True, text=True,
        cwd=_DATA_PIPELINE_DIR,
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

        local_path, minio_key = SOURCE_CONFIG[source]

        # 1. Convert records to DataFrame
        new_df = pd.DataFrame(records)
        new_df["kafka_published_at"] = published_at

        # 2. Append to local bronze parquet (transform reads from here)
        combined_df = _append_to_local(local_path, new_df)

        # 3. Upload updated bronze to MinIO (durable backup)
        _upload_to_minio(self.s3, combined_df, MINIO_BUCKET_RAW, minio_key)

        # 4. Re-run Silver→Gold transform
        _run_transform(source)

        # 5. Update Feast online store
        _update_feast()

        print(f"[consumer] Pipeline complete for source '{source}'.")


if __name__ == "__main__":
    consumer = DiseaseConsumer()
    consumer.run()
