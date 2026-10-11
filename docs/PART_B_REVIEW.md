# GridWatch Reference Architecture — Review

**Reviewer:** Raj Prajapati  
**Document reviewed:** `gridwatch-reference-architecture-DRAFT.md` (v0.3)  
**Review date:** 2026-10-10

---

## Review Summary

This document reviews the v0.3 draft as I would before it goes to a client or to an engineering team for implementation. I've ranked findings by **blast radius**: how quickly the problem would hurt the business, and how hard it would be to fix after the system is built. Architecture decisions baked into an early build are expensive to undo.

---

## Findings, Ranked by Severity

---

### 🔴 CRITICAL — Fix Before Any Code is Written

---

#### Finding 1: Multi-tenancy is broken at the API layer

**Location:** §6 (Multi-Tenancy)

**The problem:**
> "The Dashboard API scopes queries by the `operator_id` that the frontend sends as a query parameter, e.g. `GET /sites?operator_id=acme`."

This means any operator — or any attacker who has obtained a valid session token — can enumerate another operator's chargers, sessions, fault history, and invoice data by simply changing `?operator_id=` in the request. There is no enforcement of which `operator_id` a token is allowed to see.

**Blast radius:** Catastrophic. CPO data (session history, revenue data, charger locations, fault patterns) is commercially sensitive. A data leak between CPOs would breach GDPR (for German operators), destroy trust, and likely kill the product.

**Fix:**
1. `operator_id` must come from the authenticated JWT claim, not from a query parameter. The backend extracts it from the validated token and enforces it on every query:
   ```java
   // In SecurityConfig or a filter:
   String operatorId = jwtToken.getClaim("operator_id");
   // Pass to service layer — NEVER from request parameter
   ```
2. Enable PostgreSQL Row-Level Security (RLS) as a database-level enforcement backstop. Even if the application layer has a bug, the database refuses cross-tenant queries:
   ```sql
   ALTER TABLE sites ENABLE ROW LEVEL SECURITY;
   CREATE POLICY operator_isolation ON sites
     USING (operator_id = current_setting('app.operator_id'));
   ```
3. Penetration test this boundary before launch.

**What I'd need to verify:** Whether Keycloak is already configured to issue `operator_id` in the JWT, or whether this needs to be configured as a custom claim mapper.

---

#### Finding 2: RPO = 0 claim is false with async replication

**Location:** §9 (High Availability & Disaster Recovery)

**The problem:**
> "PostgreSQL runs with streaming replication in asynchronous mode to a warm standby... RPO: 0 (no data loss)."

Asynchronous replication has a non-zero replication lag — typically 10–500 ms under normal load, potentially seconds under high write pressure. If the primary fails, any WAL that has been acknowledged to the application but not yet shipped to the standby is lost. The RPO is equal to the replication lag at the moment of failure, which is never zero with async replication.

Claiming RPO = 0 on an async replica in a client-facing document is a contractual liability. If an operator asks about it during a contract negotiation and later experiences data loss, this claim is actionable.

**Blast radius:** High. Legal and contractual exposure; and if RPO is actually important to the business, the architecture doesn't deliver it.

**Fix — Option A (simpler, honest):** Change to synchronous replication for the replica in the same AZ. This makes RPO = 0 honest. Performance cost: ~1–3 ms additional write latency (the primary waits for the replica to confirm receipt). Acceptable for this workload.

**Fix — Option B (operationally pragmatic):** Keep async replication, be honest about the RPO. State: "RPO ≤ 5 s (async replication lag)" in the architecture document. Add WAL archiving to S3 (point-in-time recovery) as a second recovery mechanism. This is what I'd recommend — it's honest and operationally simpler.

**Fix — do not do:** Keep async replication and claim RPO = 0. This is the current state and must change.

---

#### Finding 3: Single-node PostgreSQL cannot sustain 5,000 writes/s of raw telemetry at stated sizing

**Location:** §10 (Capacity Sizing) and §4.2 (Telemetry)

**The problem:**
The draft proposes a single `4 vCPU / 8 GB RAM / 1 TB SSD` PostgreSQL instance receiving 5,000 writes/s of telemetry into a single unpartitioned table.

At 5,000 writes/s, a `4 vCPU` instance operating as a primary (no parallelism benefit on OLTP writes, WAL generation overhead, index maintenance) will saturate. Community benchmarks for plain PostgreSQL on comparable hardware show single-table write ceilings of 2,000–4,000 rows/s under realistic workloads with JSONB and index maintenance. The safety margin is zero or negative before adding consumer reads, dashboard queries, and settlement.

Separately: the claim "we keep all telemetry indefinitely... storage is cheap" alongside "864 GB/day" means 315 TB/year. A 1 TB SSD is full in **just over 24 hours** of production data.

**Blast radius:** The system would fall over within hours of production load.

**Fix:**
1. Use **TimescaleDB** (PostgreSQL extension) with hypertable partitioning on `event_ts`. TimescaleDB's columnar compression achieves ~10× compression on time-series data (so 864 GB/day → ~86 GB/day compressed), and its chunk-based architecture handles 10,000+ writes/s on comparable hardware.
2. Add a **retention policy** (13 months balances historical analytics needs against storage cost).
3. Right-size the database: 16 vCPU, 64 GB RAM, 8 TB NVMe SSD (primary), with a synchronous replica.
4. Separate **OLTP entities** (sites, chargers, sessions, invoices) from **time-series telemetry** at the schema level (different hypertable vs regular tables), even if they share the same PostgreSQL instance.

---

### 🟠 HIGH — Fix Before Production Launch

---

#### Finding 4: Nearest-charger query uses Pythagorean approximation — wrong at real distances

**Location:** §5.1 (Map)

**The problem:**
```sql
ORDER BY (s.latitude - :lat)^2 + (s.longitude - :lng)^2
```

This computes squared Euclidean distance in degrees, not metres. This is incorrect for two reasons:

1. **Degrees are not equal in distance.** 1° of longitude in India (~20°N latitude) ≈ 103 km, while 1° of latitude ≈ 111 km. A charger 10 km north and one 10 km east would return the eastern charger as "nearer" even if it's actually further.
2. **No spatial index is used.** Without PostGIS `<->` operator or `ST_DWithin`, this performs a full table scan on every nearest-charger query. At 12,000 sites and 50,000 chargers, this is immediately slow.

**Blast radius:** Incorrect nearest-charger results delivered to end-users (operators directing drivers to the wrong charger). Also, full-table-scan performance degrades as the dataset grows.

**Fix:**
```sql
-- Use PostGIS KNN operator with geography type (true great-circle distance)
SELECT c.charger_id, s.name,
  ST_Distance(s.location::geography,
    ST_SetSRID(ST_MakePoint(:lng, :lat), 4326)::geography) AS distance_m
FROM connectors con
JOIN chargers c ON con.charger_id = c.charger_id
JOIN sites s ON c.site_id = s.site_id
WHERE con.operator_id = :operator_id
  AND con.status = 'Available'
  AND s.location IS NOT NULL
ORDER BY s.location::geography <-> ST_SetSRID(ST_MakePoint(:lng, :lat), 4326)::geography
LIMIT 10;
```

The `<->` KNN operator uses the GIST index; no full-table scan.

---

#### Finding 5: No coordinates in vendor feed — sites table has lat/lng columns but no geocoding plan

**Location:** §4.1 (Operational tables)

**The problem:**
> "`sites` stores `latitude` and `longitude` as `double precision` columns, plus the address string from the vendor."

The field notes in the pilot data explicitly state: **"Sites carry an address string and NO latitude/longitude."** There is no `lat`/`lng` field in the CSMS feed. The draft adds lat/lng columns but provides no mechanism to populate them.

If sites have `NULL` coordinates: the map renders no markers, the nearest-charger query returns no results, and the density analytics are empty. The core product does not work.

**Blast radius:** The map — which is described as "the heart of the product" — is completely non-functional without this being solved.

**Fix:**
- Implement an async geocoding service that triggers when a new site is ingested.
- Use Nominatim (OSM, free) for India and Germany with a fallback to Google Maps Geocoding API.
- Track `geocode_status` per site (`pending` / `done` / `failed`) and surface sites with `geocode_status = 'pending'` as "location unknown" on the map rather than omitting them entirely.
- Cache geocoding results (addresses rarely change).

**What I'd need to verify:** Which geocoding providers are approved given the data (operator addresses may be considered business-sensitive; Google Maps Geocoding sends addresses to Google's servers). May need on-premises geocoding for GEC/contractual reasons.

---

#### Finding 6: Session settlement is not idempotent — double-billing risk

**Location:** §7 (Sessions & Settlement)

**The problem:**
> "Because each session produces exactly one `session.stop` event, no deduplication is needed on the settlement side — one stop event, one invoice."

This is incorrect. The webhook is **at-least-once** delivery (explicitly stated in the brief). A `session.stop` event will arrive more than once. The draft architecture runs the Settlement Consumer on every arrival of a `session.stop`, which means:

- Duplicate `session.stop` webhook → duplicate invoice → CPO billed twice for the same session.

Additionally, the brief includes a specific stub (2b) showing a duplicate `session.stop` delivery as a test case. This is a known, documented failure mode.

**Blast radius:** Double-billing any CPO would be a serious contractual breach and an immediate trust-killer.

**Fix:**
- `invoices` table must have `UNIQUE (session_id)` constraint.
- Use `INSERT INTO invoices ... ON CONFLICT (session_id) DO NOTHING` — the second arrival of the same `session.stop` is a no-op.
- Add an idempotency key check before any settlement computation.

---

#### Finding 7: No handling of out-of-order events — stale status can overwrite current status

**Location:** §3 (Ingestion) and §5.2 (Dashboard tiles)

**The problem:**
The draft consumers write status updates in the order events arrive from Kafka. The webhook is explicitly "may be delivered out of order." The CSMS stubs include a test case (2c) where `status=Available` at 14:33:40 arrives **before** `status=Charging` at 14:31:50. If the consumer writes in arrival order, the status is set to `Charging` (the stale event) after `Available` (the newer event), which is wrong.

**Blast radius:** Operators see incorrect charger status on the map. A charger shown as "Charging" when it's actually available means operators dispatch drivers to occupied chargers.

**Fix:**
```sql
-- Only update if the incoming event is newer than what we have
UPDATE connectors 
SET status = $new_status, status_ts = $event_ts
WHERE charger_id = $charger_id 
  AND connector_id = $connector_id
  AND (status_ts IS NULL OR status_ts < $event_ts);
```

This is a compare-and-swap on the event timestamp, not the arrival time.

---

### 🟡 MEDIUM — Fix Before Launch or in First Sprint

---

#### Finding 8: Redis single-node is a single point of failure for live status

**Location:** §8 (Caching & Sessions)

**The problem:**
> "We run one Redis instance; Redis is fast and a single node comfortably handles our load."

The live-status cache keyed by `charger_id` is in this single Redis instance. If it fails, all live status is lost. The draft says users re-authenticate and the cache repopulates from the next poll cycle (30 s). But:

1. All 50,000 chargers need their status repopulated — that's 50,000 Redis writes in < 30 s from one poll cycle. This may cause a write thundering-herd.
2. During the gap, the dashboard shows no live status, which is a visible product outage.

**Fix:** Redis Cluster (3 primary shards × 1 replica = 6 nodes). Redis Cluster provides both HA and horizontal write scaling. At the data volume described (< 50 MB for live status), this is operationally trivial and cost-effective.

---

#### Finding 9: No webhook signature verification

**Location:** §11 (Security)

**The problem:**
The draft mentions webhook validation against a "shared secret in the request header" but gives no detail on the algorithm. The CSMS stubs specify `X-CSMS-Signature: hmac-sha256(body, shared_secret)`. Without verifying this signature, the webhook endpoint will accept events from any source — including spoofed events that could manipulate charger status or inject fake sessions.

**Fix:** Explicitly implement HMAC-SHA256 verification:
```java
boolean isValid = MessageDigest.isEqual(
  HmacUtils.hmacSha256(sharedSecret, requestBody),
  Hex.decodeHex(signatureHeader)
);
if (!isValid) return ResponseEntity.status(401).build();
```
Reject and log (but do not 500) on invalid signatures.

---

#### Finding 10: Observability gap — no freshness KPI instrumentation

**Location:** §12 (Observability)

**The problem:**
The observability section covers host metrics, DB connections, and HTTP error rates. There is **no instrumentation to prove the contractual freshness KPIs are being met**. The platform is sold on freshness; the only way to know if you're meeting the SLA is to measure it end-to-end.

**Fix:** Instrument the full event pipeline with timestamps at each stage (CSMS event_ts → webhook_received → Kafka_published → consumer_processed → Redis_written → WebSocket_pushed). Emit a `gridwatch.e2e.freshness.seconds` histogram with P99.9 alerting against the KPI thresholds. This is the primary SLA-proving metric; host CPU metrics are secondary.

---

### 🟢 LOW — Clarification Needed / Future Phase

---

#### Finding 11: Map `GET /sites` polling at 10 s is architecturally wrong at scale

The dashboard client polls `GET /sites` every 10 s for the full GeoJSON. At 12,000 sites × N connected clients × 10 s interval, this is an API and database hammering pattern. Should be replaced with WebSocket push of status diffs. (Medium priority — matters at scale, not in a pilot.)

---

#### Finding 12: Settlement consumer reads from `session.stop` on Kafka, but settlement depends on tariff state at session time, not processing time

If a tariff changes while a session is in flight, or during Kafka consumer lag, the tariff used for settlement should be the one **at the time of session start**, not at processing time. The draft doesn't address this. Needs a `tariff_snapshot` at `session.start` or a tariff versioning table. (Low priority — depends on whether tariffs change frequently.)

---

#### Finding 13: Manual failover on PostgreSQL primary failure is inconsistent with 99.9% availability target

RTO of 5 minutes with manual promotion means a single primary failure = 5 minutes of downtime = the entire monthly availability budget consumed in one event. At 99.9% target, automated failover (Patroni or equivalent) is required. Manual human-on-call promotion cannot reliably achieve < 5 minutes RTO at 3am.

---

## Summary Table

| # | Finding | Severity | When to Fix |
|---|---|---|---|
| 1 | Tenant isolation via query param — data leak risk | 🔴 CRITICAL | Before any code |
| 2 | RPO = 0 claim false with async replication | 🔴 CRITICAL | Before any code |
| 3 | Single 4 vCPU PG cannot sustain 5K writes/s; 1TB fills in 24h | 🔴 CRITICAL | Before any code |
| 4 | Nearest-charger uses Pythagorean approximation — incorrect + full scan | 🟠 HIGH | Before production |
| 5 | No geocoding plan; sites have no coordinates from vendor | 🟠 HIGH | Before production |
| 6 | Settlement not idempotent — double-billing on duplicate webhook | 🟠 HIGH | Before production |
| 7 | Out-of-order events can overwrite newer status with stale status | 🟠 HIGH | Before production |
| 8 | Single Redis node — SPOF for live status | 🟡 MEDIUM | Before launch |
| 9 | Webhook signature verification unspecified | 🟡 MEDIUM | Before launch |
| 10 | No freshness KPI instrumentation — can't prove SLA | 🟡 MEDIUM | First sprint |
| 11 | Full GeoJSON polling at 10s — won't scale | 🟢 LOW | Scale phase |
| 12 | Settlement tariff not snapshotted at session start | 🟢 LOW | Before go-live |
| 13 | Manual PG failover inconsistent with 99.9% availability target | 🟢 LOW | Before production |

The three critical findings (tenant isolation, RPO claim, database sizing) would each independently cause the platform to fail commercially or technically. They must be resolved before a line of implementation code is written.
