import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from streaming.config import KAFKA_BOOTSTRAP_SERVERS, CONSUMER_GROUP_ID


class BaseConsumer:
    """
    Base Kafka consumer. Subclasses implement handle_message() to process
    each consumed message.
    """

    def __init__(self, topics: list[str], max_retries: int = 5, retry_delay: float = 5.0):
        self.topics   = topics
        self.consumer = None
        self._connect(topics, max_retries, retry_delay)

    def _connect(self, topics: list[str], max_retries: int, retry_delay: float) -> None:
        for attempt in range(1, max_retries + 1):
            try:
                self.consumer = KafkaConsumer(
                    *topics,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    group_id=CONSUMER_GROUP_ID,
                    auto_offset_reset="earliest",   # replay from start if no committed offset
                    enable_auto_commit=False,        # manual commit after successful processing
                    value_deserializer=lambda v: __import__("json").loads(v.decode("utf-8")),
                )
                print(f"[consumer] Subscribed to topics: {topics}")
                return
            except NoBrokersAvailable:
                print(f"[consumer] Kafka not available (attempt {attempt}/{max_retries}), retrying in {retry_delay}s...")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    raise

    def handle_message(self, source: str, records: list[dict], published_at: str) -> None:
        """
        Process one Kafka message. Must be implemented by subclasses.

        Args:
            source:       Source name (e.g. "ilinet")
            records:      List of record dicts from the message payload
            published_at: ISO timestamp string when the message was published
        """
        raise NotImplementedError

    def run(self) -> None:
        """
        Consume messages in a loop until interrupted.
        Commits offset only after successful processing.
        """
        print("[consumer] Starting consume loop. Press Ctrl+C to stop.")
        try:
            for msg in self.consumer:
                payload = msg.value
                source  = payload.get("source", "unknown")
                records = payload.get("records", [])
                published_at = payload.get("published_at", "")

                print(f"[consumer] Received message: source={source}, records={len(records)}, topic={msg.topic}, partition={msg.partition}, offset={msg.offset}")

                try:
                    self.handle_message(source, records, published_at)
                    self.consumer.commit()
                except Exception as e:
                    print(f"[consumer] Error handling message from {source}: {e}")
                    # Do not commit — message will be reprocessed on restart

        except KeyboardInterrupt:
            print("[consumer] Shutting down.")
        finally:
            self.close()

    def close(self) -> None:
        if self.consumer:
            self.consumer.close()
            print("[consumer] Consumer closed.")
