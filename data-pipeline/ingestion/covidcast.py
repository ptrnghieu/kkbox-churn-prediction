import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

COVIDCAST_ENDPOINT = "https://api.delphi.cmu.edu/epidata/covidcast/"

# API hard limit: 3,650 rows per request (50 states × ~73 days).
# We use 60-day chunks to stay safely under the cap.
_CHUNK_DAYS = 60

# Each entry is (data_source, signal, available_until).
# fb-survey was discontinued 2022-06-25 when Facebook ended its CMU partnership.
# Fetching beyond that date returns no data, so we cap it.
SIGNALS = [
    ("chng",          "smoothed_outpatient_flu",  None),
    ("doctor-visits", "smoothed_adj_cli",          None),
    ("fb-survey",     "smoothed_hh_cmnty_cli",     "20220625"),
]


def _fetch_signal_chunk(
    data_source: str,
    signal: str,
    start_day: str,
    end_day: str,
    geo_type: str = "state",
    max_retries: int = 3,
    retry_delay: float = 5.0,
) -> pd.DataFrame:
    """
    Fetch one COVIDcast signal for a single date chunk (≤ 60 days).

    Returns a DataFrame with columns:
        geo_value, time_value, value, stderr, sample_size, data_source, signal
    Returns an empty DataFrame if the API reports no data for this window.
    """
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(
                COVIDCAST_ENDPOINT,
                params={
                    "data_source": data_source,
                    "signal":      signal,
                    "time_type":   "day",
                    "geo_type":    geo_type,
                    "time_values": f"{start_day}-{end_day}",
                    "geo_value":   "*",  # all states
                    # No "format": "json" — that returns a raw list, breaking .get("result")
                },
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()

            # result == -2 means "no results" (valid empty response for this window)
            if data.get("result") == -2 or data.get("message") == "no results":
                return pd.DataFrame()
            if data.get("result") != 1:
                raise ValueError(f"API error [{data_source}/{signal}]: {data.get('message', 'unknown')}")

            df = pd.DataFrame(data["epidata"])
            df["data_source"] = data_source
            df["signal"]      = signal
            return df[["geo_value", "time_value", "value", "stderr", "sample_size", "data_source", "signal"]]

        except Exception as e:
            print(f"    Attempt {attempt}/{max_retries} failed ({data_source}/{signal} {start_day}-{end_day}): {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
            else:
                raise


def _fetch_signal(
    data_source: str,
    signal: str,
    start_day: str,
    end_day: str,
) -> pd.DataFrame:
    """
    Fetch one COVIDcast signal across the full date range by paginating in
    60-day chunks to stay under the API's 3,650-row-per-request hard limit.
    """
    fmt = "%Y%m%d"
    current = datetime.strptime(start_day, fmt)
    end = datetime.strptime(end_day, fmt)

    chunks = []
    while current <= end:
        chunk_end = min(current + timedelta(days=_CHUNK_DAYS - 1), end)
        s = current.strftime(fmt)
        e = chunk_end.strftime(fmt)
        chunk = _fetch_signal_chunk(data_source, signal, s, e)
        if not chunk.empty:
            chunks.append(chunk)
        current = chunk_end + timedelta(days=1)
        time.sleep(0.3)  # be polite to the API between chunks

    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def fetch_covidcast(
    start_day: str = "20200301",
    output_path: str = "data/raw/covidcast.parquet",
) -> pd.DataFrame:
    """
    Fetch all configured COVIDcast signals (state-level) and save to bronze parquet.

    Paginates in 60-day chunks to respect the API's 3,650-row-per-request limit.
    Each row in the output represents one (state, date, signal) observation.
    geo_value is the 2-letter state abbreviation (e.g. "ca", "ny").

    Args:
        start_day:   First date to fetch in 'YYYYMMDD' format.
        output_path: Destination path for the raw bronze parquet.

    Returns:
        Raw concatenated DataFrame across all signals.
    """
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    print(f"Fetching COVIDcast signals: {start_day} → {today}")

    frames = []
    for data_source, signal, available_until in SIGNALS:
        end_day = min(today, available_until) if available_until else today
        print(f"  Fetching {data_source}/{signal} ({start_day} → {end_day}) ...")
        df = _fetch_signal(data_source, signal, start_day, end_day)
        if df.empty:
            print(f"    0 rows (no data returned)")
        else:
            frames.append(df)
            print(f"    {len(df):,} rows")
        time.sleep(0.5)

    if not frames:
        raise RuntimeError("No data returned from any COVIDcast signal.")

    combined = pd.concat(frames, ignore_index=True)
    print(f"Total: {len(combined):,} rows across {len(SIGNALS)} signals")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    combined.to_parquet(output_path, index=False)
    print(f"Saved bronze → {output_path}")
    return combined


if __name__ == "__main__":
    fetch_covidcast()
