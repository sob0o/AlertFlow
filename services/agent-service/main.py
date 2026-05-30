import json
import uuid
import random
from datetime import datetime

from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer

app = FastAPI(title="agent-service")
from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app) 

def get_producer():
    return KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

class Transaction(BaseModel):
    merchant_id: str
    amount: float
    currency: str
    status: str
    latency_ms: int

@app.get("/health")
def health():
    return {"status": "ok", "service": "agent-service"}

@app.post("/inject")
def inject(transaction: Transaction):
    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        **transaction.model_dump()
    }
    producer = get_producer()
    producer.send("payment.events", event)
    producer.flush()
    return {"status": "published", "event": event}

@app.post("/simulate")
def simulate():
    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "merchant_id": random.choice(["acme_pay", "swift_bank", "lydia"]),
        "amount": round(random.uniform(1.0, 999.0), 2),
        "currency": "EUR",
        "status": random.choice(["success", "success", "success", "failed"]),
        "latency_ms": random.randint(20, 800)
    }
    producer = get_producer()
    producer.send("payment.events", event)
    producer.flush()
    return {"status": "published", "event": event}