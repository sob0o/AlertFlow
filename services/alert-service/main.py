import json
import time
import threading
from datetime import datetime

from fastapi import FastAPI
from kafka import KafkaConsumer
from prometheus_fastapi_instrumentator import Instrumentator

from database import init_db, SessionLocal, AlertRecord

app = FastAPI(title="alert-service")
Instrumentator().instrument(app).expose(app)

alerts = []

RULES = {
    "success_rate": {"warning": 95.0, "critical": 90.0},
    "p99_latency": {"warning": 500, "critical": 1000}
}

def evaluate(merchant_id: str, metrics: dict):
    triggered = []
    sr = metrics.get("success_rate", 100)
    p99 = metrics.get("latency", {}).get("p99", 0)

    if sr < RULES["success_rate"]["critical"]:
        triggered.append({"level": "CRITICAL", "reason": f"success_rate={sr}% < 90%"})
    elif sr < RULES["success_rate"]["warning"]:
        triggered.append({"level": "WARNING", "reason": f"success_rate={sr}% < 95%"})

    if p99 > RULES["p99_latency"]["critical"]:
        triggered.append({"level": "CRITICAL", "reason": f"p99={p99}ms > 1000ms"})
    elif p99 > RULES["p99_latency"]["warning"]:
        triggered.append({"level": "WARNING", "reason": f"p99={p99}ms > 500ms"})

    db = SessionLocal()
    for alert in triggered:
        alerts.append({
            "merchant_id": merchant_id,
            "level": alert["level"],
            "reason": alert["reason"],
            "timestamp": datetime.utcnow().isoformat()
        })
        db.add(AlertRecord(
            merchant_id=merchant_id,
            level=alert["level"],
            reason=alert["reason"]
        ))
        print(f"[ALERT] {alert['level']} - {merchant_id} - {alert['reason']}")
    db.commit()
    db.close()

def consume():
    while True:
        try:
            consumer = KafkaConsumer(
                "alert.triggers",
                bootstrap_servers="kafka:9092",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id="alert-group",
                auto_offset_reset="earliest"
            )
            for message in consumer:
                data = message.value
                evaluate(data["merchant_id"], data["metrics"])
        except Exception as e:
            print(f"Kafka not ready, retrying in 5s... ({e})")
            time.sleep(5)

@app.on_event("startup")
def startup():
    init_db()
    thread = threading.Thread(target=consume, daemon=True)
    thread.start()

@app.get("/health")
def health():
    return {"status": "ok", "service": "alert-service"}

@app.get("/alerts")
def get_alerts():
    return alerts