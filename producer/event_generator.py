import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

faker = Faker()



KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:29092"
)
TOPIC = "orders"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    enable_idempotence=True,
    acks="all",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_order():
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "order_created",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "order_id": f"ORD-{random.randint(10000, 99999)}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "customer_name": faker.name(),
        "product": random.choice(
            ["Laptop", 
            "Smartphone", 
            "Tablet", 
            "Headphones", 
            "Camera"]
        ),
        "quantity": random.randint(1, 5),
        "amount": round(random.uniform(20, 2000), 2),
        "country": faker.country(),
    }

print("Starting order event generator...")

while True:

    order = generate_order()

    producer.send(
        TOPIC,
        key=order["order_id"].encode("utf-8"),
        value=order,
    )

    print(f"Produced: {order}")

    time.sleep(2)
