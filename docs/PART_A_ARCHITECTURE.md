# GridWatch Platform — Architecture & Design Document

**Author:** Raj Prajapati  
**Date:** 2026-10-10  
**Version:** 1.0  
**Role:** Senior Technical Architect & Engineering Lead

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Context & Constraints](#2-system-context--constraints)
3. [End-to-End Data Flow](#3-end-to-end-data-flow)
4. [Storage Design](#4-storage-design)
5. [GIS Architecture](#5-gis-architecture)
6. [Freshness KPI Analysis](#6-freshness-kpi-analysis)
7. [High Availability & Disaster Recovery](#7-high-availability--disaster-recovery)
8. [Capacity Sizing](#8-capacity-sizing)
9. [Security & Multi-Tenant Isolation](#9-security--multi-tenant-isolation)
10. [Observability](#10-observability)
11. [Decision Log](#11-decision-log)
12. [What I Would Not Build (and Why)](#12-what-i-would-not-build-and-why)

---

## 1. Executive Summary

GridWatch is a multi-tenant EV network monitoring platform serving Charge Point Operators (CPOs). It ingests telemetry from vendor Charging Station Management Systems (CSMS), normalises it, and delivers a live operational picture — a GIS map, dashboards, session data, and settlement — across ~50,000 chargers in India and Germany.

The platform is sold on **data freshness**. Every architectural decision is judged against the contractual KPIs (P99.9 status < 5 s, fault < 5 s, active session < 10 s, dashboard tile < 2 s, map update P95 < 10 s) and against the two hard constraints that flow from the vendor interface: **poll-only at 30-second minimum intervals** and **webhook delivery that is best-effort, at-least-once, and out-of-order**.

The architecture uses a dual-path ingestion model (webhook for low latency, poll as a correctness backstop), a purpose-built write-optimised telemetry store (TimescaleDB), a separate OLTP store for operational entities, PostGIS for spatial queries, and Redis for the live-status layer that makes the sub-2-second dashboard possible. All freshness targets are achievable; the one I cannot guarantee is the P99.9 < 5 s fault latency if a fault event is only delivered via the poll path — the 30 s minimum poll interval makes P99.9 < 5 s impossible from poll alone. This document explains the mitigation.

---

## 2. System Context & Constraints

### 2.1 What We Control

- GridWatch ingestion services (poll scheduler, webhook receiver)
- Internal message bus (Kafka)
- Internal databases (PostgreSQL/TimescaleDB with PostGIS, Redis)
- Dashboard API and WebSocket server
- Daily settlement job

### 2.2 What We Do Not Control

- The vendor CSMS (third-party, rate-limits, has downtime, data shape is fixed)
- Charger clocks (timestamps arrive in charger local timezone with UTC offset — not normalised)
- Network delivery of webhooks (best-effort, at-least-once, batched up to 60 s, out-of-order)
- The absence of coordinates in the vendor feed (address string only — no lat/lng)

### 2.3 Key Data Quirks (These Drive Architecture)

| Quirk | Implication |
|---|---|
| Energy register is cumulative lifetime Wh | Session energy = stop_register − start_register. Must handle meter resets (stop < start). |
| Timestamps in charger local timezone with offset | Normalise to UTC on ingestion. Parse ISO 8601 with offset, store as `timestamptz`. |
| No coordinates in vendor feed | Geocode addresses at site registration time. Geocoding is async; map shows "pending" until resolved. |
| Webhook at-least-once, out-of-order | Idempotency keys on every event. Status uses last-write-wins by event timestamp, not arrival time. |
| Poll rate-limited to 30 s/site minimum | Poll is correctness backstop, not primary freshness path. Webhook carries the freshness load. |

---

## 3. End-to-End Data Flow

### 3.1 Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│  VENDOR CSMS (third-party)                                              │
│  • GET /v1/chargers?site_id={id}&since={ts}   (poll, min 30s/site)     │
│  • POST {webhook_url}  (push, at-least-once, out-of-order, 60s batch)  │
└────────────────────────────┬──────────────┬────────────────────────────┘
                             │              │
              ┌──────────────▼──┐     ┌─────▼────────────────────┐
              │  Poll Scheduler  │     │  Webhook Receiver          │
              │  (per-site       │     │  (Spring Boot, stateless)  │
              │  rate-limited,   │     │  • HMAC-SHA256 verify      │
              │  cursor-tracked) │     │  • ACK immediately (async) │
              └──────────────┬──┘     └─────┬────────────────────┘
                             │              │
                             └──────┬───────┘
                                    │ normalised CsmsEvent (JSON)
                             ┌──────▼───────────────────────────┐
                             │  Kafka topic: csms.events.raw     │
                             │  partitioned by charger_id        │
                             │  retention: 7 days                │
                             └───────────┬───────────────────────┘
                                         │
              ┌──────────────────────────┼───────────────────────┐
              │                          │                        │
     ┌────────▼──────┐        ┌──────────▼──────┐    ┌──────────▼──────┐
     │ Status &       │        │ Session &        │    │ Settlement       │
     │ Telemetry      │        │ Fault Consumer   │    │ Consumer         │
     │ Consumer       │        │                  │    │ (idempotent,     │
     │ • idempotent   │        │ • session UPSERT │    │ daily batch)     │
     │ • last-ts-wins │        │ • fault insert   │    │                  │
     └───┬────────────┘        └────────┬─────────┘    └────────┬─────────┘
         │                             │                        │
         │  live status                │  sessions/faults       │  invoices
         ▼                             ▼                        ▼
  ┌──────────────┐          ┌───────────────────────────────────────────┐
  │ Redis Cluster │          │  PostgreSQL 16 (primary + read replicas)  │
  │ (live status  │          │  • TimescaleDB extension (telemetry)      │
  │  per connector│          │  • PostGIS extension (spatial)            │
  │  TTL 60s)     │          │  • OLTP tables: operators, sites,         │
  └──────┬────────┘          │    chargers, connectors, sessions,        │
         │                   │    faults, invoices, tariffs              │
         │                   └──────────────────┬────────────────────────┘
         │                                      │
         └──────────────────────────────────────┤
                                                │
                                    ┌───────────▼────────────┐
                                    │  Dashboard API           │
                                    │  (Spring Boot,           │
                                    │   WebSocket + REST)      │
                                    │  • reads Redis for live  │
                                    │    status (< 1ms)        │
                                    │  • reads PG read replica │
                                    │    for analytics         │
                                    └───────────┬─────────────┘
                                                │
                                    ┌───────────▼─────────────┐
                                    │  Frontend (Next.js)      │
                                    │  • MapLibre GL (map)     │
                                    │  • WebSocket (live push) │
                                    │  • REST (historical)     │
                                    └─────────────────────────┘
```

### 3.2 Ingestion Detail

**Webhook path (primary freshness path):**
1. CSMS POSTs event batch to `POST /ingest/webhook/{operator_id}`
2. Receiver validates HMAC-SHA256 signature in `X-CSMS-Signature` header
3. Returns `202 Accepted` within 200 ms (before any processing — prevents CSMS redelivery storms)
4. Events published to Kafka `csms.events.raw` asynchronously
5. Status Consumer reads from Kafka, writes to Redis (live status) and TimescaleDB (telemetry)
6. Dashboard API pushes update to connected WebSocket clients

**Poll path (correctness backstop):**
1. Poll Scheduler maintains a `SitePollerState` per site: last successful poll timestamp, retry count, backoff
2. Sites polled on a distributed schedule: 50,000 chargers across ~12,000 sites, 30 s interval minimum
3. Pagination: follows `next_cursor` until absent. Handles 429 (backoff to next slot), 500 (exponential retry up to 3x), malformed events (skip event, log, continue batch)
4. Publishes to the same Kafka topic — consumers are idempotent, so duplicates from both paths are safe
5. High-water mark (last event `ts`) persisted per site in PostgreSQL to survive restarts

**Idempotency strategy:**
- Each raw CSMS event is assigned a deterministic `event_key`: `SHA256(charger_id + connector_id + type + ts_utc + type_specific_fields)`
- Kafka consumer checks `event_keys` table before processing (Bloom filter in Redis for hot path, PG for cold)
- Status updates use last-write-wins by `event_ts` (the charger's timestamp, not arrival time): `UPDATE connectors SET status = $1, status_ts = $2 WHERE charger_id = $3 AND connector_id = $4 AND status_ts < $2`
- Session UPSERT on `session_id` (PK): `INSERT ... ON CONFLICT (session_id) DO UPDATE SET ... WHERE sessions.stop_ts IS NULL` — ensures a duplicate `session.stop` does not overwrite a correctly closed session

---

## 4. Storage Design

### 4.1 Store Selection Rationale

| Store | Purpose | Why |
|---|---|---|
| PostgreSQL 16 + TimescaleDB + PostGIS | OLTP entities, telemetry, spatial | Single operational store; PostGIS for spatial queries; TimescaleDB for time-series compression and continuous aggregates on telemetry |
| Redis Cluster | Live connector/charger status | Sub-millisecond reads; the dashboard tile freshness KPI (< 2 s end-to-end) is only achievable if the status read is < 1 ms |
| Kafka | Event bus between ingestion and consumers | Durable, replayable, decouples ingestion speed from consumer speed; handles burst from 50K chargers |

### 4.2 Core Schema

```sql
-- ─── TENANCY ────────────────────────────────────────────────────────────────
CREATE TABLE operators (
  operator_id   text PRIMARY KEY,                -- e.g. "acme-charge"
  name          text NOT NULL,
  country       char(2) NOT NULL,               -- ISO 3166-1 alpha-2
  tariff_per_kwh_minor_units int NOT NULL,       -- e.g. 850 = ₹8.50/kWh
  session_fee_minor_units    int NOT NULL,
  currency      char(3) NOT NULL,               -- "INR" or "EUR"
  created_at    timestamptz DEFAULT now()
);

-- ─── SITES ──────────────────────────────────────────────────────────────────
CREATE TABLE sites (
  site_id       text PRIMARY KEY,               -- e.g. "S-IN-0007"
  operator_id   text NOT NULL REFERENCES operators,
  name          text NOT NULL,
  address       text NOT NULL,                  -- raw vendor address string
  country       char(2) NOT NULL,
  location      geometry(Point, 4326),          -- PostGIS; NULL until geocoded
  geocode_status text NOT NULL DEFAULT 'pending', -- 'pending'|'done'|'failed'
  geocoded_at   timestamptz,
  created_at    timestamptz DEFAULT now()
);
CREATE INDEX idx_sites_operator ON sites (operator_id);
CREATE INDEX idx_sites_location ON sites USING GIST (location); -- spatial index
CREATE INDEX idx_sites_geocode_pending ON sites (geocode_status) 
  WHERE geocode_status = 'pending';

-- ─── CHARGERS ───────────────────────────────────────────────────────────────
CREATE TABLE chargers (
  charger_id    text PRIMARY KEY,               -- e.g. "C-IN-0007-A"
  site_id       text NOT NULL REFERENCES sites,
  operator_id   text NOT NULL REFERENCES operators,
  model         text,
  max_kw        numeric(8,2),
  created_at    timestamptz DEFAULT now()
);
CREATE INDEX idx_chargers_site ON chargers (site_id);
CREATE INDEX idx_chargers_operator ON chargers (operator_id);

-- ─── CONNECTORS (the plug-level entity) ─────────────────────────────────────
CREATE TABLE connectors (
  charger_id    text NOT NULL REFERENCES chargers,
  connector_id  text NOT NULL,                  -- "1", "2" as from vendor
  status        text NOT NULL DEFAULT 'Unknown',
  status_ts     timestamptz,                    -- event_ts, not ingestion time
  operator_id   text NOT NULL REFERENCES operators,
  PRIMARY KEY (charger_id, connector_id)
);
CREATE INDEX idx_connectors_operator ON connectors (operator_id);
CREATE INDEX idx_connectors_status ON connectors (status, operator_id);

-- ─── TELEMETRY (TimescaleDB hypertable) ─────────────────────────────────────
CREATE TABLE telemetry (
  event_key     text NOT NULL,                  -- deduplication key
  charger_id    text NOT NULL,
  connector_id  text,
  event_ts      timestamptz NOT NULL,           -- charger time, normalised UTC
  ingested_at   timestamptz NOT NULL DEFAULT now(),
  event_type    text NOT NULL,                  -- 'status'|'meter_value'|'fault'|'session'
  status        text,
  energy_wh     bigint,                         -- cumulative lifetime register
  power_w       int,
  fault_code    text,
  fault_severity text,
  raw_payload   jsonb,                          -- keep original for debugging
  PRIMARY KEY (event_key, event_ts)             -- event_ts needed for hypertable PK
);

-- Convert to TimescaleDB hypertable, partition by week
SELECT create_hypertable('telemetry', 'event_ts',
  chunk_time_interval => INTERVAL '1 week');

-- Compression: compress chunks older than 2 hours
ALTER TABLE telemetry SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'charger_id',
  timescaledb.compress_orderby = 'event_ts DESC'
);
SELECT add_compression_policy('telemetry', INTERVAL '2 hours');

-- Retention: keep 13 months (1 month buffer past 1-year operator contractual window)
SELECT add_retention_policy('telemetry', INTERVAL '13 months');

-- Key indexes on hypertable
CREATE INDEX idx_telemetry_charger_ts ON telemetry (charger_id, event_ts DESC);
CREATE INDEX idx_telemetry_type ON telemetry (event_type, event_ts DESC)
  WHERE event_type IN ('fault');

-- ─── SESSIONS ───────────────────────────────────────────────────────────────
CREATE TABLE sessions (
  session_id       text PRIMARY KEY,
  charger_id       text NOT NULL REFERENCES chargers,
  connector_id     text NOT NULL,
  operator_id      text NOT NULL REFERENCES operators,
  start_ts         timestamptz,
  stop_ts          timestamptz,
  start_meter_wh   bigint,
  stop_meter_wh    bigint,
  energy_wh        bigint,                       -- computed: stop - start (handle reset)
  meter_reset      boolean DEFAULT false,        -- flag if stop < start
  invoice_id       text,
  created_at       timestamptz DEFAULT now(),
  updated_at       timestamptz DEFAULT now()
);
CREATE INDEX idx_sessions_operator ON sessions (operator_id, start_ts DESC);
CREATE INDEX idx_sessions_open ON sessions (operator_id) WHERE stop_ts IS NULL;

-- ─── FAULTS ─────────────────────────────────────────────────────────────────
CREATE TABLE faults (
  id              bigserial PRIMARY KEY,
  event_key       text UNIQUE NOT NULL,          -- deduplication
  charger_id      text NOT NULL REFERENCES chargers,
  connector_id    text,
  operator_id     text NOT NULL REFERENCES operators,
  fault_code      text NOT NULL,
  severity        text NOT NULL,
  event_ts        timestamptz NOT NULL,
  resolved_at     timestamptz,
  created_at      timestamptz DEFAULT now()
);
CREATE INDEX idx_faults_operator_ts ON faults (operator_id, event_ts DESC);
CREATE INDEX idx_faults_open ON faults (operator_id) WHERE resolved_at IS NULL;

-- ─── INVOICES ───────────────────────────────────────────────────────────────
CREATE TABLE invoices (
  invoice_id      text PRIMARY KEY,             -- session_id + date
  session_id      text UNIQUE NOT NULL REFERENCES sessions,
  operator_id     text NOT NULL REFERENCES operators,
  energy_wh       bigint NOT NULL,
  tariff_per_kwh  int NOT NULL,
  session_fee     int NOT NULL,
  amount_minor    bigint NOT NULL,               -- in minor units (paise/cents)
  currency        char(3) NOT NULL,
  invoice_date    date NOT NULL,
  created_at      timestamptz DEFAULT now()
);
CREATE INDEX idx_invoices_operator_date ON invoices (operator_id, invoice_date);

-- ─── POLL STATE ─────────────────────────────────────────────────────────────
CREATE TABLE site_poll_state (
  site_id         text PRIMARY KEY REFERENCES sites,
  last_polled_at  timestamptz,
  last_event_ts   timestamptz,                   -- high-water mark
  cursor          text,                          -- opaque cursor from CSMS
  consecutive_errors int DEFAULT 0,
  next_poll_at    timestamptz,
  updated_at      timestamptz DEFAULT now()
);
```

### 4.3 Redis Live-Status Schema

```
Key: status:{operator_id}:{charger_id}:{connector_id}
Value: JSON { status, status_ts, power_w, session_id }
TTL:  60 seconds (refreshed on every event; stale if no event > 60 s → fallback to PG)

Key: site_summary:{operator_id}:{site_id}
Value: JSON { total, available, charging, faulted, offline }
TTL:  15 seconds (recomputed by Status Consumer on each charger status change at that site)

Key: dedup:{event_key}
Value: "1"
TTL:  24 hours (Bloom filter substitute for hot-path deduplication)
```

### 4.4 Telemetry Continuous Aggregates (TimescaleDB)

```sql
-- 5-minute utilisation per site (feeds analytics dashboard)
CREATE MATERIALIZED VIEW site_utilisation_5m
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('5 minutes', event_ts) AS bucket,
  c.site_id,
  c.operator_id,
  COUNT(*) FILTER (WHERE t.status = 'Charging') AS charging_count,
  COUNT(*) FILTER (WHERE t.status = 'Available') AS available_count,
  AVG(t.power_w) FILTER (WHERE t.power_w > 0) AS avg_power_w
FROM telemetry t
JOIN chargers c ON t.charger_id = c.charger_id
WHERE t.event_type = 'status'
GROUP BY 1, 2, 3;

SELECT add_continuous_aggregate_policy('site_utilisation_5m',
  start_offset => INTERVAL '1 hour',
  end_offset => INTERVAL '5 minutes',
  schedule_interval => INTERVAL '5 minutes');
```

---

## 5. GIS Architecture

### 5.1 The Coordinate Problem

The vendor CSMS provides **no coordinates** — only an address string (e.g. `"Sector 18 Metro Station, Gate 3, Noida, UP 201301"`). This is the first hard architectural decision.

**Approach: Async geocoding at site registration time**

When a new site is ingested (from the first poll of a new `site_id`), it is written to `sites` with `geocode_status = 'pending'` and `location = NULL`. A `GeocoderWorker` (a Spring `@Scheduled` bean, one worker per deployment) picks up pending sites and geocodes them via the Nominatim API (OSM, free, covers India and Germany) with a fallback to Google Maps Geocoding API for low-confidence results.

On success: `UPDATE sites SET location = ST_SetSRID(ST_MakePoint($lng, $lat), 4326), geocode_status = 'done'`.

The map shows these sites with a "location pending" indicator until geocoded. In practice, for a 50-site pilot, all sites geocode within 60 seconds of first ingestion.

**Why not batch-geocode up front?** We don't have the site list before ingestion. CPOs on-board sites dynamically.

**Why Nominatim + Google fallback?** Nominatim is free, has good India and Germany coverage, and respects our privacy posture (no PII in addresses). Google catches the edge cases (unmapped addresses, new developments). We cache geocode results in Redis for 30 days to avoid repeat calls.

### 5.2 Spatial Storage

PostGIS `geometry(Point, 4326)` in the `sites` table. The GIST index on `location` supports:

- **Nearest charger query**: KNN search with PostGIS `<->` operator (true great-circle distance with `geography` type)
- **Within-region queries**: `ST_Within(location, polygon)` using the GIST index
- **Density analytics**: `ST_HexagonGrid` or hexbin aggregation by region

```sql
-- Nearest available charger to a point (correct spherical distance)
SELECT 
  c.charger_id,
  s.name AS site_name,
  s.address,
  ST_Distance(
    s.location::geography,
    ST_SetSRID(ST_MakePoint(:lng, :lat), 4326)::geography
  ) AS distance_metres
FROM connectors con
JOIN chargers c ON con.charger_id = c.charger_id
JOIN sites s ON c.site_id = s.site_id
WHERE con.operator_id = :operator_id
  AND con.status = 'Available'
  AND s.location IS NOT NULL
ORDER BY s.location::geography <-> ST_SetSRID(ST_MakePoint(:lng, :lat), 4326)::geography
LIMIT 10;

-- Charger density per 50 km² hexbin (coverage analytics)
SELECT 
  h.geom,
  COUNT(s.site_id) AS site_count,
  COUNT(c.charger_id) AS charger_count
FROM ST_HexagonGrid(0.5, ST_MakeEnvelope(68.0, 6.5, 97.4, 35.7, 4326)) h  -- India bbox
LEFT JOIN sites s ON ST_Within(s.location, h.geom) AND s.operator_id = :operator_id
LEFT JOIN chargers c ON c.site_id = s.site_id
GROUP BY h.geom;
```

### 5.3 Map Serving Architecture

The dashboard map is not a `GET /sites` polling loop. That approach collapses at 12,000 sites and 50,000 chargers.

**Architecture:**

```
Client (MapLibre GL) 
  → Initial load: GET /api/v1/map/sites/geojson?operator_id=X
    Returns: GeoJSON FeatureCollection of all sites with current status
    Served from: Redis cache (full GeoJSON blob, keyed by operator, TTL 30s)
    
  → Live updates: WebSocket /ws/live?operator_id=X
    Receives: { type: "status_change", site_id, charger_id, connector_id, status, ts }
    Only diffs — not full reloads
    Client updates the map marker colour without re-fetching the full GeoJSON
```

**Why WebSocket over polling?** The map freshness KPI is P95 < 10 s. With WebSocket, updates are pushed within 1–3 seconds of the Status Consumer writing to Redis. With 10-second client polling, we hit P95 = 10 s on the nose and P99.9 = 60 s+ (CSMS batch window). WebSocket is the right call.

**GeoJSON cache invalidation:** The full operator GeoJSON is rebuilt when any charger status in that operator's network changes. At 50,000 chargers × 1 event/10 s = 5,000 events/s, this would blow through the cache. Instead: individual marker updates go via WebSocket; the cached GeoJSON is rebuilt every 30 s as a background job and served to newly-connected clients. Existing clients stay current via WebSocket diffs.

### 5.4 Germany/India Cross-Timezone Handling on the Map

Timestamps arrive in charger local timezone (`+05:30` India, `+01:00`/`+02:00` Germany with DST). All timestamps are normalised to UTC on ingestion. The map shows "last seen" in the operator's configured timezone, not UTC. Timezone configuration is per-operator.

---

## 6. Freshness KPI Analysis

| Signal | Target | Path | Achievable? | Analysis |
|---|---|---|---|---|
| Connector/charger status | < 5 s P99.9 | Webhook → Kafka → Consumer → Redis → WebSocket | **Yes, if webhook fires** | Webhook → Redis: < 1.5 s typical. P99.9 depends on CSMS webhook reliability. |
| Active session power & energy | < 10 s P99.9 | Same webhook path | **Yes** | Meter value events in same webhook batch. |
| Fault / alarm surfaced | < 5 s P99.9 | Webhook → Kafka → Consumer → Redis → WebSocket | **Conditionally** | See §6.1 |
| Dashboard tile end-to-end | < 2 s P99.9 | WebSocket push + Redis read | **Yes** | Redis read < 1 ms; API → WebSocket < 100 ms. |
| Map live-status update | < 10 s P95 | WebSocket diff push | **Yes** | Same path as status. P95 well within target. |

### 6.1 Fault Latency — The Hard Case

**The problem:** If a fault event is only delivered via the poll API (webhook missed or not registered), the minimum latency is 30 s (one poll cycle) + processing time. This makes P99.9 < 5 s impossible from poll alone.

**Mitigation strategy:**
1. **Webhook is the primary path.** We register webhooks with every CSMS. Fault events are typically POSTed by the CSMS immediately, not batched.
2. **Webhook health monitoring.** We track webhook delivery timestamps per operator. If no webhook event has arrived in > 45 s for an operator that normally delivers every < 10 s, we alert operations and temporarily increase poll frequency to 30 s (the minimum allowed).
3. **Contractual note.** The P99.9 < 5 s fault KPI assumes reliable CSMS webhook delivery. If a CSMS is known to be webhook-only (batch-delayed), we should qualify the SLA in the CPO contract for that CSMS vendor. This is a business/commercial decision, not a pure engineering one.
4. **What I would tell Cidroy:** Meet the 5 s KPI in all documented webhook-capable CSMS integrations. Build the monitoring to detect webhook degradation. Flag the gap in the contract for CSMS vendors that do not support reliable webhook delivery.

### 6.2 Dashboard Tile Budget (< 2 s end-to-end)

```
Browser request → API               50 ms  (network + auth)
API → Redis (live status read)       1 ms  (sub-ms Redis GET)
API → response                       5 ms  (serialisation)
Response → browser                  50 ms  (network)
Browser render                      50 ms
──────────────────────────────────────────
Total (REST)                       ~156 ms  ✓ well within 2 s

WebSocket push (already-connected):
Consumer writes Redis → publishes WS event
  Kafka consumer lag               100 ms  (P99, well-tuned consumer)
  Redis write                        1 ms
  WebSocket push                    10 ms
  Browser render                    50 ms
──────────────────────────────────────────
Total (WS push)                    ~161 ms  ✓ excellent
```

The 2 s budget is for page-load, not push. Push is significantly faster.

---

## 7. High Availability & Disaster Recovery

### 7.1 Target

- **Availability:** 99.9% monthly (≤ 43.8 minutes downtime/month)
- **RPO:** 5 seconds (async replication lag; tolerable — we re-ingest from Kafka on recovery)
- **RTO:** 2 minutes (automated failover for database; < 5 minutes for full service)

### 7.2 Component HA

**PostgreSQL:**
- Primary + 1 synchronous replica + 1 asynchronous replica
- Synchronous replica in same AZ for durability; async replica in secondary AZ for DR
- Automated failover via Patroni. Human approval not required — this delays RTO unacceptably for a 99.9% target.
- WAL archiving to S3 (point-in-time recovery, 30-day retention)
- **Why not async-only?** Async replication with RPO = 0 claim (as in the draft) is contradictory. With async replication, you accept data loss on primary failure. We instead claim RPO = 5 s (async lag budget) and deliver it honestly.

**Kafka:**
- 3 brokers, replication factor 3, `min.insync.replicas = 2`
- Topic retention 7 days — this is the recovery window. If PostgreSQL is down for < 7 days, we replay from Kafka. In practice, RTO << 7 days.
- Consumer group offsets committed after successful DB write (at-least-once processing, idempotent consumers)

**Redis:**
- Redis Cluster: 3 primary shards × 1 replica each (6 nodes total)
- Redis is **not** the system of record. If the entire Redis cluster is lost: live status repopulates from Kafka replay within < 5 minutes (the TTL window). Operators see "status unknown" markers on the map during this window.
- **No persistence required on Redis.** AOF/RDB disabled for performance. Redis is a write-through cache of PostgreSQL state.

**Ingestion Services:**
- Stateless; deployed as Kubernetes Deployments (3 replicas each)
- Pod anti-affinity across AZs
- HPA on Kafka consumer lag

**Poll Scheduler:**
- Single active instance (leader election via Kubernetes lease / Spring Integration)
- Failure → new instance elected within 15 s
- Poll state persisted in PostgreSQL — no state loss on scheduler restart

### 7.3 What Happens When a Component Dies

| Component fails | Effect | Recovery |
|---|---|---|
| Webhook Receiver pod | Other pods absorb traffic (K8s service). 0 downtime. | Automatic (K8s). |
| Poll Scheduler | No polls for ≤ 15 s. Dashboard shows last known state (Redis TTL 60 s). | Automatic leader re-election. |
| Kafka broker (1 of 3) | No interruption. ISR covers. | Kafka auto-balances. |
| Redis node | Cluster re-shards to replica. Short (< 1 s) partition pause. | Automatic cluster failover. |
| PG primary | Patroni promotes replica. Services redirect. RTO ~ 30–60 s. | Automatic (Patroni). |
| PG primary + replica | Service continues (read from Kafka; writes queue in Kafka). DB restored from WAL + replay. | Manual recovery from WAL; Kafka replay fills the gap. |
| Entire AZ | Services re-schedule to secondary AZ. DB failover to async replica (RPO = async lag, typically < 5 s). | Patroni + K8s. |

---

## 8. Capacity Sizing

### 8.1 Ingestion Throughput

```
Chargers:      50,000
Poll interval: 10 s (per charger, implied by "every ~10 s" telemetry)
Events/s:      50,000 / 10 = 5,000 events/s (raw)
```

**Note:** The poll API rate limit is 30 s per **site** (not per charger). With ~12,000 sites averaging ~4 chargers each, polling per site every 30 s retrieves ~4 charger statuses per call = ~400 poll calls/s across the fleet. Each call returns multiple events (4 chargers × ~3 event types = 12 events/call). This is manageable.

**Webhook path:** Up to 5,000 events/s pushed to the webhook receiver. At average 1 KB/event: 5 MB/s inbound.

### 8.2 Kafka Sizing

```
Messages/s:      5,000
Message size:    ~1 KB (normalised event JSON)
Throughput:      5 MB/s
Partitions:      120 (by charger_id hash; allows 120 consumer threads)
Retention:       7 days × 5 MB/s × 86,400 s = ~3 TB
Brokers:         3 × m5.2xlarge (8 vCPU, 32 GB) — handles 250 MB/s per broker
Replication:     3× → total storage: 3 × 3 TB = 9 TB
```

### 8.3 TimescaleDB (Telemetry) Sizing

```
Events/s:          5,000
Row size:          ~300 bytes (compressed; TimescaleDB compression ~10× raw)
Raw write rate:    5,000 × 300 B = 1.5 MB/s
Compressed:        0.15 MB/s effective
Daily storage:     0.15 MB/s × 86,400 = ~13 GB/day compressed
Annual storage:    ~4.7 TB/year
13-month retention: ~5.3 TB

Chunk strategy: 1-week chunks × 120 charger segments per chunk
Hot chunks (last 2 hours): uncompressed, in RAM for fast writes
Warm chunks (2h–7d): compressed on disk
Cold chunks (>7d): compressed; retention policy deletes at 13 months
```

**Hardware:** 2× PostgreSQL nodes (primary + sync replica), each: 16 vCPU, 64 GB RAM, 8 TB NVMe SSD. The RAM handles the working set (last 2 hours uncompressed ≈ 0.15 MB/s × 7,200 s ≈ 1.1 GB — well within 64 GB).

### 8.4 OLTP Storage

```
Operators:     ~100          → negligible
Sites:         12,000        → ~50 MB
Chargers:      50,000        → ~200 MB
Connectors:    ~100,000      → ~400 MB
Sessions:      ~50K/day × 365 × 13 months → ~10 million rows → ~10 GB
Invoices:      1:1 with sessions                              → ~5 GB
Faults:        ~1K/day (estimate)                             → ~500 MB/year
Total OLTP:    ~25 GB (tiny; shares the TimescaleDB nodes)
```

### 8.5 Redis Sizing

```
Live status keys:  100,000 connectors × 200 B = 20 MB
Site summaries:    12,000 × 100 B = 1.2 MB
Dedup keys:        5,000 events/s × 86,400 s × 100 B (24h TTL) = 43 GB (!!)
```

**Dedup key problem:** 24-hour TTL × 5,000/s = 432M keys = 43 GB. This is too large for Redis.

**Solution:** Use a Bloom filter (Redis module `RedisBloom`, or Guava in-process) for hot-path deduplication (last 1 hour). For cold-path (older events, rare duplicates from poll), check the `event_key` column in TimescaleDB with a covering index. The Bloom filter fits in < 100 MB for 1-hour window at 0.1% false positive rate.

**Redis cluster total:** ~200 MB live status + ~50 MB Bloom filter + ~500 MB overhead = 1 GB. Very comfortable on 3 × 8 GB nodes.

### 8.6 API Tier

```
Connected dashboard clients: ~500 concurrent (estimate; WebSocket connections)
REST requests/s:             ~2,000 (page loads, historical queries)
API nodes:                   4 × Spring Boot pods, 4 vCPU / 8 GB each
```

---

## 9. Security & Multi-Tenant Isolation

### 9.1 Authentication & Authorisation

- Operators authenticate via **Keycloak** (OAuth2 + OIDC). JWTs contain `operator_id` claim.
- Dashboard API enforces `operator_id` from the JWT — not from a query parameter. (The draft architecture's `?operator_id=acme` is a critical flaw; see Part B.)
- Service-to-service authentication via mutual TLS within the cluster.
- Webhook endpoint validated via HMAC-SHA256 of the request body against a per-operator shared secret stored in HashiCorp Vault (not in application config).

### 9.2 Multi-Tenant Data Isolation

**Database level:**
- Every OLTP row carries `operator_id` FK.
- Row-Level Security (RLS) enabled on all operational tables. Application role `gridwatch_api` can only see rows where `operator_id = current_setting('app.operator_id')`.
- TimescaleDB hypertable: `operator_id` is a segmentby column in compression, ensuring operator data lands in separate chunks.
- No cross-tenant joins possible at the API layer (even if the application code has a bug, RLS enforces it at DB level).

**Kafka level:**
- Topics are not per-tenant (impractical at 100+ operators). Instead, consumer groups filter by `operator_id` from the event payload. Kafka ACLs prevent application services from consuming topics they shouldn't.

**Redis level:**
- Key namespace includes `operator_id` — no possibility of key collision between tenants.

### 9.3 Network Security

- All ingress TLS-terminated at a load balancer (ALB/NLB).
- Database, Kafka, Redis on private subnets. No public egress except to geocoding API (egress via NAT Gateway with IP allowlisting on the geocoding provider side).
- Webhook receiver on a public-facing endpoint; rate-limited per operator (100 req/s per operator; CSMS bursts above this are queued in an SQS buffer).

### 9.4 Secrets Management

- HashiCorp Vault for webhook shared secrets, database passwords, geocoding API keys.
- Kubernetes secrets backed by Vault via the Vault Agent Injector.
- No secrets in environment variables, ConfigMaps, or source code.

---

## 10. Observability

### 10.1 Proving the Freshness KPIs

This is the hardest observability problem: how do we know we're meeting P99.9 < 5 s for status freshness?

**Instrumentation chain:**

```
CSMS event_ts (charger clock)
    ↓
  webhook_received_at (ingestion service wall clock)   → webhook_ingestion_delay = received - event_ts
    ↓
  kafka_published_at                                   → kafka_publish_delay = published - received
    ↓
  consumer_processed_at                               → consumer_delay = processed - published
    ↓
  redis_written_at                                    → redis_write_delay = written - processed
    ↓
  websocket_pushed_at                                 → push_delay = pushed - written
    ↓
  client_rendered_at (browser performance.now())     → client_render_delay (from client telemetry)
```

Each stage emits a Micrometer histogram metric:
- `gridwatch.ingestion.webhook.delay.seconds` (labels: operator_id, event_type)
- `gridwatch.ingestion.kafka.publish.delay.seconds`
- `gridwatch.consumer.processing.delay.seconds`
- `gridwatch.api.websocket.push.delay.seconds`
- `gridwatch.e2e.freshness.seconds` = sum of all stages (computed in consumer)

**SLA breach alerts (Alertmanager):**
```yaml
- alert: FreshnessKPIBreach
  expr: histogram_quantile(0.999, gridwatch_e2e_freshness_seconds_bucket) > 5
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "P99.9 end-to-end freshness > 5s for {{ $labels.event_type }}"
```

**Prometheus + Grafana** dashboard with:
- P50/P95/P99/P99.9 freshness latency per event type, per operator
- Kafka consumer lag per partition (leading indicator of delay)
- Poll scheduler: sites polled/min, sites behind schedule, retry rate
- Redis hit rate on live-status reads
- Webhook delivery rate vs poll event rate (ratio shows CSMS webhook health)

### 10.2 Operational Dashboards

| Dashboard | Key Metrics |
|---|---|
| Platform Health | Kafka lag, DB connections, error rate, pod restarts |
| Freshness SLA | P50/P95/P99.9 freshness per KPI bucket, breach count |
| Ingestion | Webhook events/s, poll calls/s, 429 rate, 500 rate, malformed event rate |
| GIS | Geocoding queue depth, geocoding success rate, geocoding latency |
| Settlement | Sessions closed/day, invoices generated, meter reset flags |

### 10.3 Structured Logging

All services emit JSON logs (Logback + Logstash encoder) with:
- `trace_id`, `span_id` (OpenTelemetry distributed tracing)
- `operator_id`, `charger_id`, `event_type`
- No PII in logs

---

## 11. Decision Log

### Decision 1: Dual-Path Ingestion (Webhook + Poll) vs Webhook-Only vs Poll-Only

**Options:**
- A) Poll-only (30 s/site minimum). Simple, but P99.9 < 5 s status KPI is **impossible** — minimum latency from poll = 30 s.
- B) Webhook-only. Meets freshness KPIs, but webhook delivery is best-effort. A missed webhook = silent staleness.
- C) Dual-path: Webhook for freshness, poll for correctness. Complex, but both requirements met.

**Decision: C (Dual-path).**

The KPIs are contractual. Missing them has revenue and reputational consequences. The added complexity (idempotent consumers, deduplication) is manageable and well-understood. The poll backstop catches events the webhook misses and ensures eventual consistency within 30 s even if webhooks fail completely.

---

### Decision 2: TimescaleDB vs InfluxDB vs Cassandra for Telemetry

**Options:**
- A) PostgreSQL plain table with partitioning. Simple, but 5,000 writes/s on a plain table saturates without heroic tuning; range scans slow.
- B) InfluxDB. Native time-series, excellent write throughput. Separate operational concern; no SQL joins with OLTP data; licensing changed unfavourably.
- C) Cassandra. Excellent write throughput and scale-out. Heavy operational overhead; no spatial queries; SQL unfamiliar to most engineers.
- D) TimescaleDB (PostgreSQL extension). Time-series optimised, columnar compression, continuous aggregates, PostGIS compatible, native SQL. Single database to operate.

**Decision: D (TimescaleDB).**

Operational simplicity wins. One PostgreSQL cluster handles OLTP, time-series, and spatial queries. The team already knows SQL. TimescaleDB's automatic chunking + compression handles 5,000 writes/s comfortably on reasonable hardware (benchmarked at 10K+ writes/s on equivalent hardware). The joins between telemetry and OLTP entities are native SQL.

---

### Decision 3: WebSocket vs Long-Poll vs Client Polling for Live Status

**Options:**
- A) Client polls every 10 s. Simple. P95 latency = 10 s. P99.9 = one poll cycle + CSMS batch delay. Misses the P99.9 < 5 s target.
- B) Server-Sent Events (SSE). Simpler than WebSocket; unidirectional. Sufficient for dashboard push but doesn't handle bidirectional control messages (future: operator sends commands to charger).
- C) WebSocket. Bidirectional, persistent connection, sub-second push. More complex to operate (sticky sessions or distributed pub-sub).
- D) Long-poll. Compatible with HTTP/1.1. Higher per-request overhead than WebSocket at scale.

**Decision: C (WebSocket)** with Spring WebSocket + Redis pub-sub for horizontal scaling.

WebSocket is the only option that meets the P99.9 freshness targets while also leaving room for future bidirectional use cases (charger commands, operator alerts). Redis pub-sub means any API pod can push to any connected client regardless of which pod they connected to.

---

### Decision 4: Row-Level Security vs Application-Layer Tenant Filtering

**Options:**
- A) Application enforces `operator_id` filter in every query. Simple, but one bug = data leak between tenants.
- B) Database-level RLS. Belt-and-suspenders: even if the application has a bug, the database enforces isolation.
- C) Separate schema or database per tenant. Strongest isolation; operationally impractical at 100+ CPOs.

**Decision: B (RLS) + A (application filtering) as defence in depth.**

For a multi-tenant platform with contractual data isolation obligations, application-only filtering is not sufficient. RLS is the backstop. The performance overhead (< 5% on indexed queries) is acceptable.

---

## 12. What I Would Not Build (and Why)

**1. Demand forecasting models.** The brief explicitly calls this out as architectural-understanding-only. The right approach is: export aggregated session and utilisation data from TimescaleDB to a data warehouse (BigQuery/Redshift) on a daily schedule, and run ML models there. Do not embed ML in the operational platform — it couples unrelated concerns and creates operational risk.

**2. A message broker other than Kafka.** RabbitMQ and SQS are simpler, but Kafka's replay capability is operationally critical: if a consumer has a bug, or a database is down, we replay from Kafka rather than losing events. This is worth the operational overhead.

**3. A separate microservice per domain from day one.** The brief asks for a multi-tenant monitoring platform, not a microservices showcase. Starting with a modular monolith (clear package boundaries within one deployment) and extracting services when a module's scaling requirements diverge is the right call at this stage. Premature extraction creates distributed systems problems (network calls, data consistency, distributed transactions) before the team has learned the domain.

**4. Per-CPO database schemas.** Attractive for isolation, unmanageable at 100+ CPOs. RLS delivers the isolation; a schema-per-tenant approach would require 100+ schema migrations on every schema change.

**5. Real-time ML anomaly detection in the hot path.** Rule-based threshold alerts (offline charger > 5 min, fault not resolved > 1 hr) cover 80% of the operational value. ML anomaly detection can be added as an async batch process against the TimescaleDB continuous aggregates, not in the ingestion hot path.
