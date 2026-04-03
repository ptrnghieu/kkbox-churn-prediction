"""
COVIDcast producer — fetches the last 3 days of signals and publishes to Kafka.

Runs daily (triggered by Airflow). Fetches 3 days to handle typical 1-2 day lag.
"""
import requests
from datetime import datetime, timezone, timedelta

from kafka.config import TOPICS
from kafka.producers.base_producer import BaseProducer

COVIDCAST_ENDPOINT = "https://api.delphi.cmu.edu/epidata/covidcast/"

SIGNALS = [
    ("chng",          "smoothed_outpatient_flu"),
    ("doctor-visits", "smoothed_adj_cli"),
    ("fb-survey",     "smoothed_hh_cmnty_cli"),
]


class COVIDcastProducer(BaseProducer):
    source = "covidcast"
    topic  = TOPICS["covidcast"]

    def fetch_latest(self) -> list[dict]:
        """Fetch the last 3 days of all COVIDcast signals for all states."""
        today     = datetime.now(timezone.utc).date()
        start_day = (today - timedelta(days=3)).strftime("%Y%m%d")
        end_day   = today.strftime("%Y%m%d")

        all_records: dict[tuple, dict] = {}  # (geo_value, time_value) → record

        for data_source, signal in SIGNALS:
            resp = requests.get(
                COVIDCAST_ENDPOINT,
                params={
                    "data_source": data_source,
                    "signal":      signal,
                    "time_type":   "day",
                    "geo_type":    "state",
                    "time_values": f"{start_day}-{end_day}",
                    "geo_value":   "*",
                },
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("result") != 1:
                print(f"[covidcast] No results for {data_source}/{signal}: {data.get('message')}")
                continue

            col = f"{data_source.replace('-', '_')}_{signal}"
            for row in data.get("epidata", []):
                key = (row["geo_value"], row["time_value"])
                if key not in all_records:
                    all_records[key] = {
                        "region_id":  row["geo_value"].lower(),
                        "date":       str(row["time_value"]),
                    }
                all_records[key][col] = row.get("value")

        records = list(all_records.values())
        print(f"[covidcast] Fetched {len(records)} records for {start_day}→{end_day}")
        return records


if __name__ == "__main__":
    producer = COVIDcastProducer()
    try:
        producer.publish()
    finally:
        producer.close()
