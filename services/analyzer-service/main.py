import json
import time
import threading
from collections import defaultdict

import redis
from fastapi import FastAPI
from kafka import KafkaConsumer, KafkaProducer
from prometheus_fastapi_instrumentator import Instrumentator

from database import init_db, SessionLocal, MetricSnapshot

app = FastAPI(title="analyzer-service")
Instrumentator().instrument(app).expose(app)

metrics_store = defaultdict(list)
cache = redis.Redis(host="redis", port=6379, decode_responses=True)

def get_producer():
    return KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def compute_metrics(events):
    total = len(events)
    success = sum(1 for e in events if e["status"] == "success")
    latencies = sorted(e["latency_ms"] for e in events)
    return {
        "total_transactions": total,
        "success_rate": round(success / total * 100, 2),
        "latency": {
            "p50": latencies[int(total * 0.50)],
            "p95": latencies[int(total * 0.95)],
            "p99": latencies[int(total * 0.99)]
        }
    }

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
            producer = get_producer()
            for message in consumer:
                event = message.value
                merchant_id = event["merchant_id"]
                metrics_store[merchant_id].append(event)

                metrics = compute_metrics(metrics_store[merchant_id])

                # Cache dans Redis (expire après 60s)
                cache.setex(
                    f"metrics:{merchant_id}",
                    60,
                    json.dumps(metrics)
                )

                # Persist dans PostgreSQL
                db = SessionLocal()
                db.add(MetricSnapshot(
                    merchant_id=merchant_id,
                    total_transactions=metrics["total_transactions"],
                    success_rate=metrics["success_rate"],
                    p50=metrics["latency"]["p50"],
                    p95=metrics["latency"]["p95"],
                    p99=metrics["latency"]["p99"]
                ))
                db.commit()
                db.close()

                producer.send("alert.triggers", {"merchant_id": merchant_id, "metrics": metrics})
                producer.flush()

        except Exception as e:
            print(f"Error, retrying in 5s... ({e})")
            time.sleep(5)

@app.on_event("startup")
def startup():
    init_db()
    thread = threading.Thread(target=consume, daemon=True)
    thread.start()

@app.get("/health")
def health():
    return {"status": "ok", "service": "analyzer-service"}

@app.get("/metrics/{merchant_id}")
def get_metrics(merchant_id: str):
    # Cherche dans Redis d'abord
    cached = cache.get(f"metrics:{merchant_id}")
    if cached:
        return {"source": "cache", "merchant_id": merchant_id, **json.loads(cached)}

    # Fallback sur la mémoire
    events = metrics_store.get(merchant_id, [])
    if not events:
        return {"merchant_id": merchant_id, "message": "no data yet"}

    return {"source": "memory", "merchant_id": merchant_id, **compute_metrics(events)}