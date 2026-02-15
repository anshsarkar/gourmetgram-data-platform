#!/usr/bin/env python3
"""
Viral Event Simulator

Publishes a burst of view events for a real image from the database,
simulating viral content to trigger the inference service.

Usage:
    python3 simulate_viral.py                      # pick a random image
    python3 simulate_viral.py <image_id>           # use a specific image UUID
"""
import json
import sys
import time
import random
import psycopg2
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:19092"
VIEWS_TOPIC = "gourmetgram.views"
DATABASE_URL = "postgresql://user:password@localhost:5432/gourmetgram"

BURST_VIEWS = 110   # just over the viral threshold of 100 in 5 min


def get_random_image_id():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM images ORDER BY RANDOM() LIMIT 1")
            row = cur.fetchone()
            if not row:
                print("No images found in the database. Run the data generator first.")
                sys.exit(1)
            return str(row[0])
    finally:
        conn.close()


def main():
    image_id = sys.argv[1] if len(sys.argv) > 1 else get_random_image_id()
    print(f"Simulating viral burst for image: {image_id}")
    print(f"Publishing {BURST_VIEWS} view events to '{VIEWS_TOPIC}'...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    now = time.time()
    for i in range(BURST_VIEWS):
        # Spread events across the last 4 minutes so they all fall within the 5-min window
        event_time = now - random.uniform(0, 240)
        event = {
            "image_id": image_id,
            "user_id": f"sim-user-{i}",
            "timestamp": event_time,
            "event_type": "view",
        }
        producer.send(VIEWS_TOPIC, value=event)

    producer.flush()
    producer.close()

    print(f"Done. {BURST_VIEWS} view events published.")
    print("")
    print("Watch the stream consumer detect the viral alert:")
    print("  docker logs -f stream-consumer")
    print("")
    print("Then watch the inference service make a decision:")
    print("  docker logs -f inference-service")


if __name__ == "__main__":
    main()
