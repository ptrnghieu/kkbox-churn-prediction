import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from streaming.config import KAFKA_BOOTSTRAP_SERVERS


class BaseProducer:
    """
    Base Kafka producer. Subclasses implement fetch_latest() to return
    the records to publish for one production cycle.

    Each message published has the envelope:
        {
            "source":       <str>  source name (e.g. "ilinet")
            "published_at": <str>  UTC ISO timestamp
            "records":      <list> list of dicts, one per (region, period)
        }
    """

    source: str = ""   # set by subclass
    topic:  str = ""   # set by subclass

    def __init__(self, max_retries: int = 5, retry_delay: float = 5.0):
        self.producer = None
        self._connect(max_retries, retry_delay)

    def _connect(self, max_retries: int, retry_delay: float) -> None:
        for attempt in range(1, max_retries + 1):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                    acks="all",           # wait for all replicas
                    retries=3,
                )
                print(f"[{self.source}] Connected to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
                return
            except NoBrokersAvailable:
                print(f"[{self.source}] Kafka not available (attempt {attempt}/{max_retries}), retrying in {retry_delay}s...")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    raise

    def fetch_latest(self) -> list[dict]:
        """
        Fetch the most recent data records for this source.
        Must be implemented by each subclass.
        Returns a list of dicts (one per region/period).
        """
        raise NotImplementedError

    def publish(self) -> int:
        """
        Fetch latest records and publish them as a single Kafka message.
        Returns the number of records published.
        """
        records = self.fetch_latest()
        if not records:
            print(f"[{self.source}] No new records to publish.")
            return 0

        message = {
            "source":       self.source,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "records":      records,
        }

        future = self.producer.send(self.topic, message)
        self.producer.flush()
        future.get(timeout=10)   # raise if delivery failed

        print(f"[{self.source}] Published {len(records)} records → topic '{self.topic}'")
        return len(records)

    def close(self) -> None:
        if self.producer:
            self.producer.close()
            print(f"[{self.source}] Producer closed.")
