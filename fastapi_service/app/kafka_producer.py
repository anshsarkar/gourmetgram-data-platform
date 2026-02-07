import os
import json
import logging
from typing import Dict, Any, Optional
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

logger = logging.getLogger(__name__)


class KafkaProducerManager:

    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.enabled = True  # Set to False if Kafka is not available

    async def start(self):
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Fire-and-forget configuration
                acks=1,  # Wait for leader acknowledgment
                max_in_flight_requests_per_connection=5,
                # Enable idempotence to prevent duplicates
                enable_idempotence=True,
                # Timeout settings
                request_timeout_ms=30000,
                api_version='auto'
            )

            await self.producer.start()
            logger.info(f"Kafka producer started successfully. Bootstrap servers: {self.bootstrap_servers}")

        except Exception as e:
            logger.error(f"Failed to start Kafka producer: {e}")
            logger.warning("Kafka publishing will be disabled. API will continue to work.")
            self.enabled = False
            self.producer = None

    async def stop(self):
        if self.producer:
            try:
                await self.producer.stop()
                logger.info("Kafka producer stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping Kafka producer: {e}")

    async def publish_event(self, topic: str, event_data: Dict[str, Any]) -> bool:
        if not self.enabled or not self.producer:
            logger.debug(f"Kafka disabled. Event not published to {topic}: {event_data.get('event_type', 'unknown')}")
            return False

        try:
            # Send event asynchronously without waiting for response
            await self.producer.send(topic, value=event_data)
            logger.debug(f"Published event to {topic}: {event_data.get('event_type', 'unknown')}")
            return True

        except KafkaError as e:
            logger.error(f"Kafka error publishing to {topic}: {e}")
            return False

        except Exception as e:
            logger.error(f"Unexpected error publishing to {topic}: {e}")
            return False


# Global singleton instance
kafka_producer = KafkaProducerManager()


async def get_kafka_producer() -> KafkaProducerManager:
    return kafka_producer
