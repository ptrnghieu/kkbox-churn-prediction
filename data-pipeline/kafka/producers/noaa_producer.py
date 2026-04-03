"""
NOAA (Open-Meteo) producer — fetches the last 3 days of weather and publishes to Kafka.

Runs daily (triggered by Airflow). Fetches 3 days to handle any lag.
"""
import time
import requests
from datetime import datetime, timezone, timedelta

from kafka.config import TOPICS
from kafka.producers.base_producer import BaseProducer

OPEN_METEO_ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"

STATE_COORDS: dict[str, tuple[float, float]] = {
    "al": (32.3617, -86.2792),  "ak": (58.3005, -134.4197), "az": (33.4484, -112.0740),
    "ar": (34.7465,  -92.2896), "ca": (38.5816, -121.4944), "co": (39.7392, -104.9903),
    "ct": (41.7637,  -72.6851), "de": (39.1582,  -75.5244), "fl": (30.4518,  -84.2727),
    "ga": (33.7490,  -84.3880), "hi": (21.3069, -157.8583), "id": (43.6150, -116.2023),
    "il": (39.7984,  -89.6544), "in": (39.7684,  -86.1581), "ia": (41.5868,  -93.6250),
    "ks": (39.0473,  -95.6752), "ky": (38.2009,  -84.8733), "la": (30.4515,  -91.1871),
    "me": (44.3106,  -69.7795), "md": (38.9784,  -76.4922), "ma": (42.3601,  -71.0589),
    "mi": (42.7325,  -84.5555), "mn": (44.9537,  -93.0900), "ms": (32.2988,  -90.1848),
    "mo": (38.5767,  -92.1735), "mt": (46.5958, -112.0270), "ne": (40.8136,  -96.7026),
    "nv": (39.1638, -119.7674), "nh": (43.2081,  -71.5376), "nj": (40.2171,  -74.7429),
    "nm": (35.6870, -105.9378), "ny": (42.6526,  -73.7562), "nc": (35.7796,  -78.6382),
    "nd": (46.8083, -100.7837), "oh": (39.9612,  -82.9988), "ok": (35.4676,  -97.5164),
    "or": (44.9429, -123.0351), "pa": (40.2732,  -76.8867), "ri": (41.8240,  -71.4128),
    "sc": (34.0007,  -81.0348), "sd": (44.3683, -100.3510), "tn": (36.1627,  -86.7816),
    "tx": (30.2672,  -97.7431), "ut": (40.7608, -111.8910), "vt": (44.2601,  -72.5754),
    "va": (37.5407,  -77.4360), "wa": (47.0379, -122.9007), "wv": (38.3498,  -81.6326),
    "wi": (43.0731,  -89.4012), "wy": (41.1400, -104.8202),
}


class NOAAProducer(BaseProducer):
    source = "noaa"
    topic  = TOPICS["noaa"]

    def fetch_latest(self) -> list[dict]:
        """Fetch the last 3 days of weather for all 50 state capitals."""
        today      = datetime.now(timezone.utc).date()
        start_date = (today - timedelta(days=3)).strftime("%Y-%m-%d")
        end_date   = today.strftime("%Y-%m-%d")

        records = []
        for state, (lat, lon) in STATE_COORDS.items():
            try:
                resp = requests.get(
                    OPEN_METEO_ENDPOINT,
                    params={
                        "latitude":   lat,
                        "longitude":  lon,
                        "start_date": start_date,
                        "end_date":   end_date,
                        "daily":      "temperature_2m_max,temperature_2m_min,precipitation_sum",
                        "timezone":   "UTC",
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                daily = resp.json().get("daily", {})

                for date, tmax, tmin, prcp in zip(
                    daily.get("time", []),
                    daily.get("temperature_2m_max", []),
                    daily.get("temperature_2m_min", []),
                    daily.get("precipitation_sum", []),
                ):
                    records.append({
                        "region_id": state,
                        "date":      date,
                        "tmax":      tmax,
                        "tmin":      tmin,
                        "prcp":      prcp,
                    })

                time.sleep(1)   # Open-Meteo rate limit

            except Exception as e:
                print(f"[noaa] Failed to fetch {state}: {e}")
                continue

        print(f"[noaa] Fetched {len(records)} records for {start_date}→{end_date}")
        return records


if __name__ == "__main__":
    producer = NOAAProducer()
    try:
        producer.publish()
    finally:
        producer.close()
