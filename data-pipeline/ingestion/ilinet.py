import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone

STATES = [
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga",
    "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
    "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj",
    "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc",
    "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy"
]

ILINET_ENDPOINT = "https://api.delphi.cmu.edu/epidata/fluview/"


def get_current_epiweek() -> str:
    """Return current epiweek as 'YYYYWW' string using UTC time."""
    now = datetime.now(timezone.utc)
    week = now.isocalendar()[1]
    return f"{now.year}{week:02d}"


def fetch_ilinet(
    start_epiweek: str = "201001",
    output_path: str = "data/raw/ilinet.parquet",
    max_retries: int = 3,
    retry_delay: float = 5.0,
) -> pd.DataFrame:
    """
    Fetch raw ILINet data from Delphi Epidata API and save to bronze parquet.

    Saves the raw API response as-is (all original columns intact) to output_path.
    No transformations are applied here — that happens in transforms/ilinet_transform.py.

    Args:
        start_epiweek: First epiweek to fetch in 'YYYYWW' format.
        output_path:   Destination path for the raw bronze parquet.
        max_retries:   Number of retry attempts on API failure.
        retry_delay:   Seconds to wait between retries.

    Returns:
        Raw DataFrame from the API.
    """
    end_epiweek = get_current_epiweek()
    print(f"Fetching ILINet: {start_epiweek} → {end_epiweek}")

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(
                ILINET_ENDPOINT,
                params={
                    "regions": ",".join(STATES),
                    "epiweeks": f"{start_epiweek}-{end_epiweek}",
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()

            if data["result"] != 1:
                raise ValueError(f"API error: {data.get('message', 'unknown')}")

            df = pd.DataFrame(data["epidata"])
            print(f"Fetched {len(df):,} rows × {len(df.columns)} cols")

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
    fetch_ilinet()
