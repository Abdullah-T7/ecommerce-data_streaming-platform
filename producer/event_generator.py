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
    "localhost:29092",
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    enable_idempotence=True,
    acks="all",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def generate_order():
    order_id = f"ORD-{random.randint(10000, 99999)}"
    customer_id = f"CUST-{random.randint(1000, 9999)}"
    amount = round(random.uniform(20, 2000), 2)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "order_created",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "order_id": order_id,
        "customer_id": customer_id,
        "customer_name": faker.name(),
        "product": random.choice(
            ["Laptop", "Smartphone", "Tablet", "Headphones", "Camera"]
        ),
        "quantity": random.randint(1, 5),
        "amount": amount,
        "country": faker.country(),
    }


def generate_payment(order):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "payment_completed",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "amount": order["amount"],
        "payment_method": random.choice(["card", "paypal", "bank_transfer"]),
        "payment_status": "completed",
    }


def generate_shipment(order):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "shipment_created",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "carrier": random.choice(["DHL", "FedEx", "UPS"]),
        "tracking_number": f"TRK-{random.randint(100000000, 999999999)}",
        "shipment_status": "created",
    }


print("Starting ecommerce event generator...")

while True:
    order = generate_order()
    payment = generate_payment(order)
    shipment = generate_shipment(order)

    producer.send(
        "orders",
        key=order["order_id"].encode("utf-8"),
        value=order,
    )

    producer.send(
        "payments",
        key=payment["order_id"].encode("utf-8"),
        value=payment,
    )

    producer.send(
        "shipments",
        key=shipment["order_id"].encode("utf-8"),
        value=shipment,
    )

    producer.flush()

    print(f"Produced order, payment, and shipment for {order['order_id']}")

    time.sleep(2)