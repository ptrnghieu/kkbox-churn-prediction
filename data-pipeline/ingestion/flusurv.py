import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone

FLUSURV_ENDPOINT = "https://api.delphi.cmu.edu/epidata/flusurv/"

# Confirmed valid FluSurv-NET location codes (verified against the Delphi API).
# These are the CDC surveillance catchment areas (~9% of US population).
# network_all = national aggregate used as fallback for uncovered states.
LOCATIONS = [
    "network_all",
    "CA", "CO", "CT", "GA", "MD", "MI", "MN",
    "NM", "NY_albany", "NY_rochester", "OH", "OR", "TN", "UT",
]


def _get_current_epiweek() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.year}{now.isocalendar()[1]:02d}"


def fetch_flusurv(
    start_epiweek: str = "201001",
    output_path: str = "data/raw/flusurv.parquet",
    max_retries: int = 3,
    retry_delay: float = 5.0,
) -> pd.DataFrame:
    """
    Fetch FluSurv-NET lab-confirmed flu hospitalization rates from Delphi Epidata API.

    Fetches the 14 state-level catchment areas plus network_all (national aggregate).
    In the transform, states without direct coverage are assigned the network_all rate.

    Args:
        start_epiweek: First epiweek to fetch in 'YYYYWW' format.
        output_path:   Destination path for the raw bronze parquet.

    Returns:
        Raw DataFrame with one row per (location, epiweek).
    """
    end_epiweek = _get_current_epiweek()
    print(f"Fetching FluSurv-NET: epiweeks {start_epiweek} → {end_epiweek}")

    # Fetch all locations in a single request (comma-separated)
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(
                FLUSURV_ENDPOINT,
                params={
                    "locations": ",".join(LOCATIONS),
                    "epiweeks":  f"{start_epiweek}-{end_epiweek}",
                },
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("result") != 1:
                raise ValueError(f"API error: {data.get('message', 'unknown')}")

            df = pd.DataFrame(data["epidata"])
            print(f"Fetched {len(df):,} rows across {df['location'].nunique()} locations")

            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_parquet(output_path, index=False)
            print(f"Saved bronze → {output_path}")
            return df

        except Exception as e:
            print(f"Attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
            else:
                raise


if __name__ == "__main__":
    fetch_flusurv()
