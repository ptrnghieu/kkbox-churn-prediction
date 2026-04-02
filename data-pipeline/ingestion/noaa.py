import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone
from requests import HTTPError

# Open-Meteo archive API — free, no key required, returns daily aggregates by lat/lon.
OPEN_METEO_ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"

# State capital coordinates used as representative weather locations.
# Key = 2-letter lowercase state code, value = (latitude, longitude)
STATE_COORDS: dict[str, tuple[float, float]] = {
    "al": (32.3617, -86.2792),   # Montgomery
    "ak": (58.3005, -134.4197),  # Juneau
    "az": (33.4484, -112.0740),  # Phoenix
    "ar": (34.7465,  -92.2896),  # Little Rock
    "ca": (38.5816, -121.4944),  # Sacramento
    "co": (39.7392, -104.9903),  # Denver
    "ct": (41.7637,  -72.6851),  # Hartford
    "de": (39.1582,  -75.5244),  # Dover
    "fl": (30.4518,  -84.2727),  # Tallahassee
    "ga": (33.7490,  -84.3880),  # Atlanta
    "hi": (21.3069, -157.8583),  # Honolulu
    "id": (43.6150, -116.2023),  # Boise
    "il": (39.7984,  -89.6544),  # Springfield
    "in": (39.7684,  -86.1581),  # Indianapolis
    "ia": (41.5868,  -93.6250),  # Des Moines
    "ks": (39.0473,  -95.6752),  # Topeka
    "ky": (38.2009,  -84.8733),  # Frankfort
    "la": (30.4515,  -91.1871),  # Baton Rouge
    "me": (44.3106,  -69.7795),  # Augusta
    "md": (38.9784,  -76.4922),  # Annapolis
    "ma": (42.3601,  -71.0589),  # Boston
    "mi": (42.7325,  -84.5555),  # Lansing
    "mn": (44.9537,  -93.0900),  # Saint Paul
    "ms": (32.2988,  -90.1848),  # Jackson
    "mo": (38.5767,  -92.1735),  # Jefferson City
    "mt": (46.5958, -112.0270),  # Helena
    "ne": (40.8136,  -96.7026),  # Lincoln
    "nv": (39.1638, -119.7674),  # Carson City
    "nh": (43.2081,  -71.5376),  # Concord
    "nj": (40.2171,  -74.7429),  # Trenton
    "nm": (35.6870, -105.9378),  # Santa Fe
    "ny": (42.6526,  -73.7562),  # Albany
    "nc": (35.7796,  -78.6382),  # Raleigh
    "nd": (46.8083, -100.7837),  # Bismarck
    "oh": (39.9612,  -82.9988),  # Columbus
    "ok": (35.4676,  -97.5164),  # Oklahoma City
    "or": (44.9429, -123.0351),  # Salem
    "pa": (40.2732,  -76.8867),  # Harrisburg
    "ri": (41.8240,  -71.4128),  # Providence
    "sc": (34.0007,  -81.0348),  # Columbia
    "sd": (44.3683, -100.3510),  # Pierre
    "tn": (36.1627,  -86.7816),  # Nashville
    "tx": (30.2672,  -97.7431),  # Austin
    "ut": (40.7608, -111.8910),  # Salt Lake City
    "vt": (44.2601,  -72.5754),  # Montpelier
    "va": (37.5407,  -77.4360),  # Richmond
    "wa": (47.0379, -122.9007),  # Olympia
    "wv": (38.3498,  -81.6326),  # Charleston
    "wi": (43.0731,  -89.4012),  # Madison
    "wy": (41.1400, -104.8202),  # Cheyenne
}


def _fetch_state(
    state: str,
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    max_retries: int = 3,
    retry_delay: float = 5.0,
) -> pd.DataFrame:
    """
    Fetch daily weather for one state capital from Open-Meteo archive API.

    Returns a DataFrame with columns: state, date, tmax, tmin, prcp.
    Units: tmax/tmin in °C, prcp in mm.
    """
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": start_date,
        "end_date":   end_date,
        "daily":      "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone":   "UTC",
    }

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(OPEN_METEO_ENDPOINT, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            break
        except HTTPError as e:
            wait = 60 if e.response is not None and e.response.status_code == 429 else retry_delay
            print(f"    Attempt {attempt}/{max_retries} failed ({state}): {e} — waiting {wait}s")
            if attempt < max_retries:
                time.sleep(wait)
            else:
                raise
        except Exception as e:
            print(f"    Attempt {attempt}/{max_retries} failed ({state}): {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
            else:
                raise

    daily = data.get("daily", {})
    df = pd.DataFrame({
        "state": state,
        "date":  daily.get("time", []),
        "tmax":  daily.get("temperature_2m_max", []),
        "tmin":  daily.get("temperature_2m_min", []),
        "prcp":  daily.get("precipitation_sum", []),
    })
    return df


def fetch_noaa(
    start_date: str = "2018-01-01",
    output_path: str = "data/raw/noaa.parquet",
) -> pd.DataFrame:
    """
    Fetch daily weather summaries for all 50 US state capitals via Open-Meteo.

    Open-Meteo is a free, open-source weather API with no API key requirement.
    Data source: ERA5 reanalysis / GHCND blend depending on location/date.

    Each row in the output is one (state, date) daily observation.
    Aggregation and feature engineering happen in transforms/noaa_transform.py.

    Args:
        start_date:  ISO date string for the earliest date to fetch.
        output_path: Destination path for the raw bronze parquet.

    Returns:
        Raw daily DataFrame with columns: state, date, tmax, tmin, prcp.
    """
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"Fetching weather (Open-Meteo): {start_date} → {end_date}")

    frames = []
    for state, (lat, lon) in STATE_COORDS.items():
        print(f"  {state.upper()} ...", end=" ", flush=True)
        df = _fetch_state(state, lat, lon, start_date, end_date)
        print(f"{len(df):,} rows")
        frames.append(df)
        time.sleep(6)  # Open-Meteo free tier: ~10 req/min

    combined = pd.concat(frames, ignore_index=True)
    print(f"Total: {len(combined):,} rows across 50 states")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    combined.to_parquet(output_path, index=False)
    print(f"Saved bronze → {output_path}")
    return combined


if __name__ == "__main__":
    fetch_noaa()
