# AlertFlow

[![CI](https://github.com/souhib-kacemi/alertflow/actions/workflows/ci.yml/badge.svg)](https://github.com/souhib-kacemi/alertflow/actions)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> Real-time payment infrastructure monitoring platform — detects latency spikes, error rate anomalies, and SLO violations across high-throughput transaction pipelines.

---

## Overview

Modern payment systems process thousands of transactions per second. A 200ms latency spike or a 0.5% error rate increase can translate to significant revenue loss and regulatory exposure.

**AlertFlow** is an event-driven monitoring platform built with production-grade patterns:

- Transaction events flow through **Apache Kafka** for durability and replay
- **SLIs** (p50/p95/p99 latency, success rate, throughput) are computed in real-time
- **SLO violations** trigger structured alerts before error budgets are exhausted
- All metrics and alerts are **persisted in PostgreSQL**
- Full observability via **Prometheus + Grafana**

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        AlertFlow Platform                        │
│                                                                  │
│  ┌──────────────┐    Kafka Topic       ┌─────────────────────┐  │
│  │agent-service │  ────────────────►  │  analyzer-service   │  │
│  │              │  payment.events      │                     │  │
│  │ Simulates &  │                      │  - SLI computation  │  │
│  │ collects     │                      │  - Anomaly detection│  │
│  │ payment      │                      │  - Persists in      │  │
│  │ events       │                      │    PostgreSQL       │  │
│  └──────────────┘                      └──────────┬──────────┘  │
│                                                   │              │
│                                          alert.triggers          │
│                                                   │              │
│                                        ┌──────────▼──────────┐  │
│                                        │    alert-service    │  │
│                                        │                     │  │
│                                        │  - SLO evaluation   │  │
│                                        │  - Alert history    │  │
│                                        │  - Persists in      │  │
│                                        │    PostgreSQL       │  │
│                                        └─────────────────────┘  │
│                                                                  │
│  ┌────────────┐  ┌────────────┐  ┌──────────────────────────┐  │
│  │ Prometheus │  │  Grafana   │  │  PostgreSQL + Kafka +    │  │
│  │ (metrics)  │  │(dashboards)│  │  Zookeeper               │  │
│  └────────────┘  └────────────┘  └──────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Services

### `agent-service` — Event Ingestion (port 8001)

Simulates a payment gateway emitting transaction events. Publishes structured events to the Kafka topic `payment.events`.

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Liveness probe |
| `/simulate` | POST | Publish a random transaction event |
| `/inject` | POST | Publish a custom transaction event |

### `analyzer-service` — SLI Engine (port 8002)

Consumes `payment.events` from Kafka. Computes rolling SLIs per merchant and globally. Persists snapshots to PostgreSQL. Publishes `alert.triggers` when thresholds are breached.

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Liveness probe |
| `/metrics/{merchant_id}` | GET | Current SLIs for a merchant |

### `alert-service` — SLO Alerting (port 8003)

Consumes `alert.triggers`. Evaluates alert rules. Persists alert history to PostgreSQL.

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Liveness probe |
| `/alerts` | GET | List all triggered alerts |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| API Framework | FastAPI + Pydantic v2 |
| ORM | SQLAlchemy 2.0 |
| Message Broker | Apache Kafka + Zookeeper |
| Database | PostgreSQL 16 |
| Observability | Prometheus, Grafana |
| Containerization | Docker, Docker Compose |
| Testing | pytest |
| Orchestration | Kubernetes (Kind) — _in progress_ |
| CI/CD | GitHub Actions — _in progress_ |
| IaC | Terraform (GCP) — _planned_ |

---

## Getting Started

**Prerequisites**: Docker Desktop, Docker Compose v2

```bash
# 1. Clone and configure
git clone https://github.com/souhib-kacemi/alertflow.git
cd alertflow
cp .env.example .env

# 2. Start the full stack
docker compose up --build

# 3. Simulate payment transactions
curl -X POST http://localhost:8001/simulate

# 4. Check computed SLIs
curl http://localhost:8002/metrics/acme_pay

# 5. Check triggered alerts
curl http://localhost:8003/alerts
```

### Inject a custom transaction

```bash
curl -X POST http://localhost:8001/inject \
  -H "Content-Type: application/json" \
  -d '{
    "merchant_id": "stripe_eu",
    "amount": 299.99,
    "currency": "EUR",
    "status": "failed",
    "latency_ms": 1500
  }'
```

### Observability UIs

| Tool | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Swagger agent-service | http://localhost:8001/docs | — |
| Swagger analyzer-service | http://localhost:8002/docs | — |
| Swagger alert-service | http://localhost:8003/docs | — |

---

## SLO Definitions

| SLI | Target | Warning | Critical |
|---|---|---|---|
| Success rate | ≥ 99.5% | < 95% | < 90% |
| p99 latency | ≤ 300ms | > 500ms | > 1000ms |

---

## Key Design Decisions

**Why Kafka instead of direct HTTP calls between services?**
Payment systems require durable, replayable event streams. Kafka decouples producers from consumers, handles backpressure naturally, and enables replay for backfilling metrics — the same pattern used at Stripe, Adyen, and Lydia.

**Why rolling SLIs instead of simple averages?**
Averages mask spikes. A 1-second burst of 2000ms latency on a 60-second window barely moves the average needle. Rolling p99 computation surfaces degradation immediately, which is how SRE teams operate.

**Why three separate services instead of a monolith?**
Each service has a distinct scaling profile: `agent-service` scales with data sources, `analyzer-service` is CPU-bound and scales with throughput, `alert-service` is IO-bound. Independent deployability also enables canary releases per service.

**Why persist to PostgreSQL if Kafka already stores events?**
Kafka is optimized for streaming, not querying. PostgreSQL enables efficient queries like "show me all CRITICAL alerts for merchant X in the last hour" or "what was the p99 latency trend over the last 7 days" — use cases that would be expensive on raw Kafka topics.

---

## Running Tests

```bash
docker compose build alert-service
docker compose run --rm alert-service pytest test_alerts.py -v
```

---

## Roadmap

- [x] Project scaffolding and Docker Compose setup
- [x] `agent-service` — Kafka event publisher
- [x] `analyzer-service` — SLI computation engine
- [x] `alert-service` — SLO alerting pipeline
- [x] PostgreSQL persistence for metrics and alerts
- [x] Prometheus + Grafana observability
- [x] Unit tests for alert evaluation logic
- [ ] Redis cache for latest metrics per merchant
- [ ] Kubernetes deployment with Kind
- [ ] GitHub Actions CI/CD pipeline
- [ ] Terraform IaC for GCP deployment

---

## Author

**Souhib Kacemi** — Software Engineer, Python / Cloud GCP

[LinkedIn](https://linkedin.com/in/souhib-kacemi) · [GitHub](https://github.com/souhib-kacemi)
