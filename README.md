# AlertFlow

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> Real-time SLI/SLO monitoring platform for payment transaction pipelines — detects latency spikes, error rate anomalies, and SLO violations before merchants notice.

---

## The Problem

You run a payment processing company. Your API handles thousands of transactions per second — every time a customer clicks "Pay" on a website, a request hits your system.

Everything looks fine on your dashboard. But somewhere in your infrastructure, 1 transaction out of 50 is silently failing for a specific merchant. Latency is climbing. Your p99 just crossed 800ms.

You find out 20 minutes later — when the merchant calls.

**That 20-minute gap is the problem AlertFlow solves.**

---

## What AlertFlow Does

AlertFlow is a monitoring platform that sits alongside a payment API and watches every transaction in real-time.

It continuously answers three questions:

- **Is the API fast enough?** — tracks p50, p95, p99 latency per merchant
- **Is the API reliable?** — tracks success rate over rolling windows
- **Are we about to breach our SLO?** — fires structured alerts before the error budget runs out

```
Payment API                    AlertFlow
────────────────               ──────────────────────────────────
Customer pays  ──── event ──►  agent-service receives transaction
API responds         │         analyzer-service computes SLIs
                     │         alert-service fires if SLO breached
                     ▼
              Engineer is paged
              before merchant calls
```

The `agent-service` simulates the transaction event stream that a real payment API would emit. In production, it would consume events directly from the payment system's Kafka topics.

---

## A Concrete Scenario

```
09:00  Normal traffic
       acme_pay → success rate 99.8%, p99 = 85ms  ✅

09:34  Database starts degrading
       acme_pay → p99 climbs to 620ms
       AlertFlow fires → WARNING: p99=620ms > 500ms

09:36  Degradation worsens
       acme_pay → p99 reaches 1350ms, success rate drops to 87%
       AlertFlow fires → CRITICAL: p99=1350ms > 1000ms
                      → CRITICAL: success_rate=87% < 90%

09:37  On-call engineer is paged automatically

09:41  Issue resolved. p99 back to 90ms  ✅
```

Without AlertFlow, the engineer finds out at 09:55 — when three merchants have already opened support tickets.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        AlertFlow Platform                        │
│                                                                  │
│  ┌──────────────┐    Kafka Topic       ┌─────────────────────┐  │
│  │agent-service │  ────────────────►  │  analyzer-service   │  │
│  │              │  payment.events      │                     │  │
│  │ Ingests &    │                      │  - p50/p95/p99      │  │
│  │ simulates    │                      │  - success rate     │  │
│  │ payment      │                      │  - persists to      │  │
│  │ events       │                      │    PostgreSQL       │  │
│  └──────────────┘                      └──────────┬──────────┘  │
│                                                   │              │
│                                          alert.triggers          │
│                                                   │              │
│                                        ┌──────────▼──────────┐  │
│                                        │    alert-service    │  │
│                                        │                     │  │
│                                        │  - evaluates SLOs   │  │
│                                        │  - fires alerts     │  │
│                                        │  - persists to      │  │
│                                        │    PostgreSQL       │  │
│                                        └─────────────────────┘  │
│                                                                  │
│  ┌────────────┐  ┌────────────┐  ┌──────────────────────────┐  │
│  │ Prometheus │  │  Grafana   │  │  PostgreSQL + Kafka       │  │
│  │ (metrics)  │  │(dashboards)│  │  + Zookeeper             │  │
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

Consumes `payment.events` from Kafka. Computes rolling SLIs per merchant. Persists snapshots to PostgreSQL. Publishes `alert.triggers` when thresholds are breached.

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Liveness probe |
| `/metrics/{merchant_id}` | GET | Current SLIs for a merchant |

### `alert-service` — SLO Alerting (port 8003)

Consumes `alert.triggers`. Evaluates alert rules against SLO thresholds. Persists alert history to PostgreSQL.

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

### Trigger a CRITICAL alert

```bash
# Inject 3 failed high-latency transactions
curl -X POST http://localhost:8001/inject \
  -H "Content-Type: application/json" \
  -d '{"merchant_id": "stripe_eu", "amount": 299.99, "currency": "EUR", "status": "failed", "latency_ms": 1500}'

# Check the alerts
curl http://localhost:8003/alerts
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
Averages mask spikes. A 1-second burst of 2000ms latency barely moves the average needle. Rolling p99 surfaces degradation immediately, which is how SRE teams operate.

**Why three separate services instead of a monolith?**
Each service has a distinct scaling profile: `agent-service` scales with data sources, `analyzer-service` is CPU-bound, `alert-service` is IO-bound. Independent deployability enables canary releases per service.

**Why persist to PostgreSQL if Kafka already stores events?**
Kafka is optimized for streaming, not querying. PostgreSQL enables efficient historical queries — "all CRITICAL alerts for merchant X in the last hour", "p99 trend over 7 days" — that would be expensive on raw Kafka topics.

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
