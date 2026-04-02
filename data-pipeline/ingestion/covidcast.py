import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone

COVIDCAST_ENDPOINT = "https://api.delphi.cmu.edu/epidata/covidcast/"

# Each entry is (data_source, signal).
# These three signals give complementary views of flu-like illness activity:
#   - CHNG: outpatient claims data (% flu diagnoses among outpatient visits)
#   - doctor-visits: % outpatient visits with CLI symptoms (smoothed)
#   - fb-survey: % households reporting CLI in community (survey-based)
SIGNALS = [
    ("chng",           "smoothed_outpatient_flu"),
    ("doctor-visits",  "smoothed_adj_cli"),
    ("fb-survey",      "smoothed_hh_cmnty_cli"),
]


def _fetch_signal(
    data_source: str,
    signal: str,
    start_day: str,
    end_day: str,
    geo_type: str = "state",
    max_retries: int = 3,
    retry_delay: float = 5.0,
) -> pd.DataFrame:
    """
    Fetch one COVIDcast signal for all US states in the given date range.

    Uses geo_type="state" (not county) to keep memory usage manageable —
    county-level data (~3000 counties × daily) would be ~13M rows and crash.
    State-level gives us 50 rows/day/signal which is plenty for our pipeline.

    Returns a DataFrame with columns:
        geo_value, time_value, value, stderr, sample_size, data_source, signal
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

            if data.get("result") != 1:
                raise ValueError(f"API error [{data_source}/{signal}]: {data.get('message', 'unknown')}")

            df = pd.DataFrame(data["epidata"])
            df["data_source"] = data_source
            df["signal"]      = signal
            return df[["geo_value", "time_value", "value", "stderr", "sample_size", "data_source", "signal"]]

        except Exception as e:
            print(f"  Attempt {attempt}/{max_retries} failed ({data_source}/{signal}): {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
            else:
                raise


def fetch_covidcast(
    start_day: str = "20200301",
    output_path: str = "data/raw/covidcast.parquet",
) -> pd.DataFrame:
    """
    Fetch all configured COVIDcast signals (state-level) and save to bronze parquet.

    Each row in the output represents one (state, date, signal) observation.
    geo_value is the 2-letter state abbreviation (e.g. "ca", "ny").

    Args:
        start_day:   First date to fetch in 'YYYYMMDD' format.
        output_path: Destination path for the raw bronze parquet.

    Returns:
        Raw concatenated DataFrame across all signals.
    """
    end_day = datetime.now(timezone.utc).strftime("%Y%m%d")
    print(f"Fetching COVIDcast signals: {start_day} → {end_day}")

    frames = []
    for data_source, signal in SIGNALS:
        print(f"  Fetching {data_source}/{signal} ...")
        df = _fetch_signal(data_source, signal, start_day, end_day)
        frames.append(df)
        print(f"    {len(df):,} rows")
        time.sleep(0.5)

    combined = pd.concat(frames, ignore_index=True)
    print(f"Total: {len(combined):,} rows across {len(SIGNALS)} signals")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    combined.to_parquet(output_path, index=False)
    print(f"Saved bronze → {output_path}")
    return combined


if __name__ == "__main__":
    fetch_covidcast()
