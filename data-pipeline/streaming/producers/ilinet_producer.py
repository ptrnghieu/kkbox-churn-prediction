"""
ILINet producer — fetches the latest 2 CDC epiweeks and publishes to Kafka.

Runs weekly (triggered by Airflow). Fetches only the 2 most recent weeks
to handle CDC's typical 1-week reporting lag.
"""
import requests
from datetime import datetime, timezone
from epiweeks import Week

from streaming.config import TOPICS
from streaming.producers.base_producer import BaseProducer

EPIDATA_ENDPOINT = "https://api.delphi.cmu.edu/epidata/fluview/"


class ILINetProducer(BaseProducer):
    source = "ilinet"
    topic  = TOPICS["ilinet"]

    def fetch_latest(self) -> list[dict]:
        """Fetch the 2 most recent CDC epiweeks for all 50 states."""
        now   = datetime.now(timezone.utc)
        current_week = Week.fromdate(now.date(), system="CDC")
        # Fetch last 2 weeks to cover CDC's 1-week reporting lag
        start = current_week - 2
        end   = current_week

        def _fmt(w): return w.year * 100 + w.week
        epiweeks = f"{_fmt(start)}-{_fmt(end)}"

        resp = requests.get(
            EPIDATA_ENDPOINT,
            params={
                "regions": "nat,hhs1,hhs2,hhs3,hhs4,hhs5,hhs6,hhs7,hhs8,hhs9,hhs10,al,ak,az,ar,ca,co,ct,de,fl,ga,hi,id,il,in,ia,ks,ky,la,me,md,ma,mi,mn,ms,mo,mt,ne,nv,nh,nj,nm,ny,nc,nd,oh,ok,or,pa,ri,sc,sd,tn,tx,ut,vt,va,wa,wv,wi,wy",
                "epiweeks": epiweeks,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("result") != 1:
            print(f"[ilinet] API warning: {data.get('message')}")
            return []

        records = []
        for row in data.get("epidata", []):
            records.append({
                "region_id":          row.get("region", "").lower(),
                "epiweek":            str(row.get("epiweek")),
                "weekly_ili_cases":   row.get("ili"),
                "total_patients":     row.get("num_patients"),
                "provider_count":     row.get("num_providers"),
                "weighted_ili_pct":   row.get("wili"),
                "ili_pct":            row.get("ili"),
            })

        print(f"[ilinet] Fetched {len(records)} records for epiweeks {epiweeks}")
        return records


if __name__ == "__main__":
    producer = ILINetProducer()
    try:
        producer.publish()
    finally:
        producer.close()
