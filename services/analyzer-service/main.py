import json
import time
import threading
from collections import defaultdict

from fastapi import FastAPI
from kafka import KafkaConsumer

app = FastAPI(title="analyzer-service")

metrics_store = defaultdict(list)

def consume():
    while True:
        try:
            consumer = KafkaConsumer(
                "payment.events",
                bootstrap_servers="kafka:9092",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id="analyzer-group",
                auto_offset_reset="earliest"
            )
            for message in consumer:
                event = message.value
                metrics_store[event["merchant_id"]].append(event)
        except Exception as e:
            print(f"Kafka not ready, retrying in 5s... ({e})")
            time.sleep(5)

@app.on_event("startup")
def startup():
    thread = threading.Thread(target=consume, daemon=True)
    thread.start()

@app.get("/health")
def health():
    return {"status": "ok", "service": "analyzer-service"}

@app.get("/metrics/{merchant_id}")
def get_metrics(merchant_id: str):
    events = metrics_store.get(merchant_id, [])
    if not events:
        return {"merchant_id": merchant_id, "message": "no data yet"}

    latencies = sorted(e["latency_ms"] for e in events)
    total = len(events)
    success = sum(1 for e in events if e["status"] == "success")

    return {
        "merchant_id": merchant_id,
        "total_transactions": total,
        "success_rate": round(success / total * 100, 2),
        "latency": {
            "p50": latencies[int(total * 0.50)],
            "p95": latencies[int(total * 0.95)],
            "p99": latencies[int(total * 0.99)]
        }
    }