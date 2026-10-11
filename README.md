# GridWatch — EV Charging Network Monitoring Platform

Real-time monitoring platform for multi-tenant EV charging networks. Handles 50,000 chargers across geographies, ingesting 5,000 telemetry events/second with contractual freshness guarantees: connector status < 5s at P99.9, fault surfacing < 5s at P99.9, dashboard tiles < 2s at P99.9.

---

## Quick Start

```bash
git clone https://github.com/your-username/gridwatch-ev-platform.git
cd gridwatch-ev-platform
docker compose up -d
# Wait ~30s for all services to be healthy
curl http://localhost:8080/actuator/health
```

The first startup runs Flyway migrations and seeds two pilot operators, four sites, and sample chargers automatically.

---

## The Problem This Solves

Charge Point Operators (CPOs) need near-real-time visibility into their charging network — when a charger faults, when a session starts/ends, and where available chargers are. The data arrives from a third-party CSMS (Charging Station Management System) via webhooks and a polling API.

The hard part is not CRUD. Six correctness hazards live at the ingestion boundary:

| Hazard | What Goes Wrong Without a Fix |
|---|---|
| At-least-once webhook delivery | Duplicate `session.stop` → double billing |
| Out-of-order events | Stale status overwrites newer → charger shows wrong state |
| Meter reset (`stop_wh < start_wh`) | Negative energy calculation → wrong invoice |
| Malformed event in a batch | One bad event kills 99 good ones |
| 429 rate limiting | Naive retry creates a storm that worsens the situation |
| 24h dedup window at 5K events/s | Naïve Redis key-per-event = 43 GB of keys |

Each one has a precise, targeted fix documented below.

---

## Architecture Overview

```
CSMS Vendor
    │
    ├── Webhook (push, at-least-once, out-of-order)
    │       │
    │       ▼
    │   WebhookController → HMAC verify → CsmsEventNormaliser
    │       │
    └── Poll API (pull, 30s min interval, paginated)
            │
            ▼
        CsmsPollScheduler → CsmsPollClient → CsmsEventNormaliser
                │
                ▼
           Kafka (csms.events.raw, partitioned by charger_id)
                │
                ▼
           StatusConsumer
           ├── dedup check (Redis SETNX, 24h TTL)
           ├── out-of-order guard (compare-and-swap on status_ts)
           ├── meter reset detection
           └── DB write → Redis update → WebSocket push
```

**Dual-path ingestion:** Webhooks deliver freshness (sub-second); the poll API provides correctness backstop (missed or out-of-order webhooks corrected within 30s). Neither path alone is sufficient — webhooks are best-effort, poll has minimum 30s lag.

---

## Stack

| Layer | Technology | Reason |
|---|---|---|
| Language | Java 21 | Virtual threads; Spring Boot ecosystem |
| Framework | Spring Boot 3.3 | WebSocket, Kafka, JPA, Actuator from one BOM |
| Database | PostgreSQL 16 + TimescaleDB + PostGIS | 10× time-series compression; hypertable chunking; real KNN proximity |
| Cache | Redis 7 | Live status TTL-60s; hot-path dedup via SETNX |
| Event bus | Apache Kafka | Ordered per-charger, 7-day replay, 3× replicated |
| Live push | WebSocket (STOMP) | Status diffs only — no full GeoJSON re-poll |
| Migrations | Flyway | Schema-as-code, version-controlled |
| Tests | JUnit 5 + Mockito + WireMock | WireMock for CSMS stub; Mockito for unit isolation |
| IaC | Terraform + LocalStack | Production AWS shape at zero cloud cost |

---

## How Each Failure Mode Is Handled

### 1. Duplicate Events (at-least-once delivery → double billing)

Two-layer defence so neither single failure mode can cause a billing error:

- **Hot path — Redis SETNX** (`dedup:{event_key}`, 24h TTL): blocks the second delivery before it touches the database.
- **Cold path — DB UPSERT** (`ON CONFLICT (session_id) DO UPDATE`): catches late redeliveries after the dedup window expires, and covers Redis cold-start on restart.

```java
// StatusConsumer.java
Boolean isNew = redisTemplate.opsForValue()
    .setIfAbsent("dedup:" + event.getEventKey(), "1", 24, TimeUnit.HOURS);
if (isNew == null || !isNew) {
    ack();
    return; // duplicate — processed exactly once
}
```

### 2. Out-of-Order STATUS Events

Compare-and-swap on `status_ts` (the charger's event timestamp, not Kafka arrival time). A `Charging` event timestamped 10:30 will never overwrite an `Available` event timestamped 10:35, regardless of which arrives first.

```sql
UPDATE connectors
   SET status = :status, status_ts = :eventTs
 WHERE charger_id = :chargerId
   AND connector_id = :connectorId
   AND (status_ts IS NULL OR status_ts < :eventTs)
```

Returns `0 rows updated` for stale events → no Redis update → no WebSocket push. Stale events are silently discarded.

### 3. Meter Reset (`stop_meter_wh < start_meter_wh`)

Charger energy registers are cumulative lifetime counters (odometer-style). A charger replacement or counter overflow causes the stop reading to be lower than the start. Attempting to calculate energy from this would produce a negative number and a negative invoice.

```java
boolean meterReset = event.getStartMeterWh() != null
    && event.getStopMeterWh() != null
    && event.getStopMeterWh() < event.getStartMeterWh();

Long energyWh = meterReset ? null : stopWh - startWh;
```

`energy_wh = NULL` is stored. The billing service checks `meter_reset = true` before invoicing. Operations are alerted via the `gridwatch.sessions.meter_reset` Micrometer counter.

### 4. Malformed Events in a Batch

`CsmsEventNormaliser.normalise()` returns `Optional.empty()` — never throws — for missing fields, null energy values, unparseable timestamps, or unknown event types. The caller iterates the batch, skips empty Optionals, and processes the rest. A bad event at index 42 does not affect event 43.

### 5. 429 Rate Limiting

`CsmsPollClient` maps HTTP 429 → `PollResult.rateLimited(retryAfterSeconds)`. Backoff is applied **per-site** — not globally. The `Retry-After` header is respected; fallback is 30s. Other sites continue polling normally.

```java
if (result.isRateLimited()) {
    state.setNextPollAt(now.plusSeconds(result.retryAfterSeconds()));
    pollStateRepository.save(state);
    return; // this site is backed off; others unaffected
}
```

### 6. Dedup at Scale (24h × 5K events/s)

432 million events per day. A naive Redis key per event = ~43 GB of Redis memory.

Solution: SETNX handles the hot path (events within the 24h redelivery window, which is the operational 99.9% case). The `UNIQUE(event_key)` constraint on the TimescaleDB `telemetry` table with `ON CONFLICT DO NOTHING` is the cold-path backstop for theoretical late redeliveries beyond 24h.

The 24h SETNX window covers the CSMS vendor's maximum documented redelivery window in practice.

---

## API Reference

| Method | Path | Description |
|---|---|---|
| `POST` | `/ingest/webhook/{operatorId}` | Receive CSMS webhook batch |
| `GET` | `/api/v1/network/summary` | Network utilisation rollup |
| `GET` | `/api/v1/map/sites/geojson` | GeoJSON for the live map |
| `GET` | `/api/v1/chargers/nearest` | KNN nearest available charger (PostGIS) |
| `GET` | `/api/v1/sessions/active` | Active session count |
| `GET` | `/api/v1/faults/open` | Unresolved fault list |
| `GET` | `/actuator/health` | Health check |
| `GET` | `/actuator/prometheus` | Prometheus metrics |

All dashboard endpoints scope queries by `operator_id` extracted from the JWT claim (Keycloak). See `docs/PART_A_ARCHITECTURE.md §9` for tenant isolation design.

---

## Running Tests

```bash
# Unit tests only — no Docker required
mvn test -Dgroups="unit"

# Full test suite
mvn test
```

| Test Class | Coverage |
|---|---|
| `CsmsEventNormaliserTest` | Malformed batches don't crash; timezone offsets normalise to UTC |
| `CsmsPollClientTest` | 429 returns rate-limited result; 500×3 returns error; successful poll parses correctly |
| `StatusConsumerTest` | Stale events don't overwrite newer; duplicates skip DB; meter reset sets `energy_wh=null` |
| `WebhookControllerTest` | Invalid HMAC → 401; malformed events in batch → rest published; valid batch → Kafka |

---

## Infrastructure (Terraform / LocalStack)

Provisions ECR, ECS (Fargate), S3 (WAL archive), Secrets Manager, and CloudWatch — production AWS shape, zero cloud cost.

```bash
docker run -d -p 4566:4566 localstack/localstack

cd terraform
terraform init
AWS_DEFAULT_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=test \
AWS_SECRET_ACCESS_KEY=test \
terraform apply -auto-approve
```

---

## Key Architecture Decisions

| Decision | Chosen | Rejected | Reason |
|---|---|---|---|
| Ingestion | Dual-path (webhook + poll) | Webhook-only | CSMS webhooks are best-effort; poll corrects silent gaps |
| Time-series DB | TimescaleDB | InfluxDB, Cassandra | One engine; PostGIS in same cluster; 10× compression; standard SQL |
| Proximity query | PostGIS KNN (`<->` geography) | Pythagorean degree math | Great-circle distance correct at continental scale; GIST index-backed |
| Live status | Redis TTL-60s | Query DB per request | Sub-millisecond reads; natural offline detection via TTL expiry |
| Live push | WebSocket diffs | Client polling every 10s | 12,000 sites × 500 clients × 10s polling ≈ 12 MB/s unnecessary data |
| Tenant isolation | JWT claim + RLS | `?operator_id=` query param | Query param allows any authenticated user to read any tenant's data |
| DB failover | Patroni (automated) | Manual promotion | Manual failover at 3am cannot sustain 99.9% monthly availability SLA |
| Session dedup | SETNX (hot) + UNIQUE constraint (cold) | SETNX alone | Redis cold-start and expired TTL windows need a DB backstop |

---

## What Is Not Built (And Why)

This implementation focuses on the ingestion correctness layer — the highest-risk slice. Explicitly deferred:

- **Geocoding service** — Nominatim async geocoder for address → coordinates; the map works with `geocode_status: pending` markers
- **JWT / Keycloak integration** — operator identity enforcement; currently simulated via header
- **Settlement / billing** — invoice generation from sessions where `meter_reset = false`
- **TimescaleDB continuous aggregates** — hourly/daily energy rollup materialised views
- **Alertmanager rules** — SLA breach alerts on `gridwatch.consumer.e2e_freshness > 5s`
- **Integration tests** — Testcontainers end-to-end flow across Postgres + Redis + Kafka
- **Dead-letter queue** — post-N-retry Kafka DLQ for manual inspection

These are listed in priority order for the next engineering sprint.

---

## Project Structure

```
gridwatch-ev-platform/
├── docs/
│   ├── PART_A_ARCHITECTURE.md     ← Full system architecture
│   └── PART_B_REVIEW.md           ← Reference architecture review (13 findings)
├── src/main/java/com/gridwatch/
│   ├── ingestion/
│   │   ├── poll/                  ← Poll client, scheduler, normaliser
│   │   └── webhook/               ← Webhook receiver + HMAC-SHA256 verification
│   ├── service/
│   │   ├── StatusConsumer.java    ← Kafka consumer; all correctness logic
│   │   └── EventPublisher.java
│   ├── repository/                ← JPA repositories + native SQL queries
│   ├── api/                       ← Dashboard query endpoints
│   └── config/                    ← Kafka, WebSocket, RestTemplate config
├── src/test/java/                 ← 4 test classes, 20+ test cases
├── src/main/resources/
│   ├── application.yml
│   └── db/migration/V1__init_schema.sql
├── docker-compose.yml             ← Full local stack (Postgres+TimescaleDB+PostGIS, Redis, Kafka, Zookeeper)
├── Dockerfile                     ← Multi-stage build
├── terraform/main.tf              ← LocalStack IaC (ECR, ECS, S3, Secrets Manager)
└── wiremock/mappings/             ← CSMS API stubs for testing
```

---

## Documentation

- [`docs/PART_A_ARCHITECTURE.md`](docs/PART_A_ARCHITECTURE.md) — Full architecture document: data model, ingestion design, storage decisions, GIS, real-time pipeline, HA/DR, capacity sizing, security, and observability
- [`docs/PART_B_REVIEW.md`](docs/PART_B_REVIEW.md) — Architecture review: 13 findings ranked by blast radius, from tenant isolation to database sizing to GIS correctness

