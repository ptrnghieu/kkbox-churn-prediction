"""
FluSurv-NET producer — fetches the latest 2 epiweeks and publishes to Kafka.

Runs weekly (triggered by Airflow). Fetches 2 weeks to handle reporting lag.
"""
import requests
from datetime import datetime, timezone
from epiweeks import Week

from kafka.config import TOPICS
from kafka.producers.base_producer import BaseProducer

FLUSURV_ENDPOINT = "https://api.delphi.cmu.edu/epidata/flusurv/"

LOCATIONS = [
    "network_all",
    "CA", "CO", "CT", "GA", "MD", "MI", "MN",
    "NM", "NY_albany", "NY_rochester", "OH", "OR", "TN", "UT",
]


class FluSurvProducer(BaseProducer):
    source = "flusurv"
    topic  = TOPICS["flusurv"]

    def fetch_latest(self) -> list[dict]:
        """Fetch the 2 most recent FluSurv epiweeks for all valid locations."""
        now          = datetime.now(timezone.utc)
        current_week = Week.fromdate(now.date(), system="CDC")
        start        = current_week - 2
        end          = current_week
        epiweeks     = f"{int(start)}-{int(end)}"

        resp = requests.get(
            FLUSURV_ENDPOINT,
            params={
                "locations": ",".join(LOCATIONS),
                "epiweeks":  epiweeks,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("result") != 1:
            print(f"[flusurv] API warning: {data.get('message')}")
            return []

        records = []
        for row in data.get("epidata", []):
            records.append({
                "location":    row.get("location"),
                "epiweek":     str(row.get("epiweek")),
                "rate_overall": row.get("rate_overall"),
                "rate_age_0":   row.get("rate_age_0"),
                "rate_age_1":   row.get("rate_age_1"),
                "rate_age_2":   row.get("rate_age_2"),
                "rate_age_3":   row.get("rate_age_3"),
                "rate_age_4":   row.get("rate_age_4"),
            })

        print(f"[flusurv] Fetched {len(records)} records for epiweeks {epiweeks}")
        return records


if __name__ == "__main__":
    producer = FluSurvProducer()
    try:
        producer.publish()
    finally:
        producer.close()
