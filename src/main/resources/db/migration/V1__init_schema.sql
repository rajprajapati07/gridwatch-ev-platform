-- GridWatch initial schema
-- Requires: PostgreSQL 16 + TimescaleDB + PostGIS extensions

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ─── OPERATORS (tenants) ────────────────────────────────────────────────────
CREATE TABLE operators (
    operator_id              TEXT PRIMARY KEY,
    name                     TEXT NOT NULL,
    country                  CHAR(2) NOT NULL,
    tariff_per_kwh_minor     INTEGER NOT NULL DEFAULT 0,
    session_fee_minor        INTEGER NOT NULL DEFAULT 0,
    currency                 CHAR(3) NOT NULL DEFAULT 'INR',
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ─── SITES ──────────────────────────────────────────────────────────────────
CREATE TABLE sites (
    site_id          TEXT PRIMARY KEY,
    operator_id      TEXT NOT NULL REFERENCES operators(operator_id),
    name             TEXT NOT NULL,
    address          TEXT NOT NULL,
    country          CHAR(2) NOT NULL,
    location         GEOMETRY(Point, 4326),               -- NULL until geocoded
    geocode_status   TEXT NOT NULL DEFAULT 'pending',     -- pending|done|failed
    geocoded_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_sites_operator ON sites (operator_id);
CREATE INDEX idx_sites_location ON sites USING GIST (location);
CREATE INDEX idx_sites_geocode_pending ON sites (geocode_status) WHERE geocode_status = 'pending';

-- ─── CHARGERS ───────────────────────────────────────────────────────────────
CREATE TABLE chargers (
    charger_id   TEXT PRIMARY KEY,
    site_id      TEXT NOT NULL REFERENCES sites(site_id),
    operator_id  TEXT NOT NULL REFERENCES operators(operator_id),
    model        TEXT,
    max_kw       NUMERIC(8,2),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_chargers_site ON chargers (site_id);
CREATE INDEX idx_chargers_operator ON chargers (operator_id);

-- ─── CONNECTORS ─────────────────────────────────────────────────────────────
CREATE TABLE connectors (
    charger_id    TEXT NOT NULL REFERENCES chargers(charger_id),
    connector_id  TEXT NOT NULL,
    operator_id   TEXT NOT NULL REFERENCES operators(operator_id),
    status        TEXT NOT NULL DEFAULT 'Unknown',
    status_ts     TIMESTAMPTZ,             -- event timestamp, not ingestion time
    power_w       INTEGER,
    PRIMARY KEY (charger_id, connector_id)
);
CREATE INDEX idx_connectors_operator ON connectors (operator_id);
CREATE INDEX idx_connectors_status ON connectors (operator_id, status);

-- ─── TELEMETRY (TimescaleDB hypertable) ─────────────────────────────────────
CREATE TABLE telemetry (
    event_key      TEXT NOT NULL,
    charger_id     TEXT NOT NULL,
    connector_id   TEXT,
    event_ts       TIMESTAMPTZ NOT NULL,
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_type     TEXT NOT NULL,
    status         TEXT,
    energy_wh      BIGINT,
    power_w        INTEGER,
    fault_code     TEXT,
    fault_severity TEXT,
    raw_payload    JSONB,
    PRIMARY KEY (event_key, event_ts)
);

-- Convert to TimescaleDB hypertable partitioned by week
SELECT create_hypertable('telemetry', 'event_ts',
    chunk_time_interval => INTERVAL '1 week',
    if_not_exists => TRUE);

-- Compression policy: compress after 2 hours
ALTER TABLE telemetry SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'charger_id',
    timescaledb.compress_orderby = 'event_ts DESC'
);
SELECT add_compression_policy('telemetry', INTERVAL '2 hours', if_not_exists => TRUE);

-- Retention: keep 13 months
SELECT add_retention_policy('telemetry', INTERVAL '13 months', if_not_exists => TRUE);

-- Indexes on hypertable
CREATE INDEX idx_telemetry_charger_ts ON telemetry (charger_id, event_ts DESC);

-- ─── SESSIONS ───────────────────────────────────────────────────────────────
CREATE TABLE sessions (
    session_id       TEXT PRIMARY KEY,
    charger_id       TEXT NOT NULL REFERENCES chargers(charger_id),
    connector_id     TEXT NOT NULL,
    operator_id      TEXT NOT NULL REFERENCES operators(operator_id),
    start_ts         TIMESTAMPTZ,
    stop_ts          TIMESTAMPTZ,
    start_meter_wh   BIGINT,
    stop_meter_wh    BIGINT,
    energy_wh        BIGINT,
    meter_reset      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_sessions_operator ON sessions (operator_id, start_ts DESC);
CREATE INDEX idx_sessions_open ON sessions (operator_id) WHERE stop_ts IS NULL;

-- ─── FAULTS ─────────────────────────────────────────────────────────────────
CREATE TABLE faults (
    id             BIGSERIAL PRIMARY KEY,
    event_key      TEXT UNIQUE NOT NULL,
    charger_id     TEXT NOT NULL REFERENCES chargers(charger_id),
    connector_id   TEXT,
    operator_id    TEXT NOT NULL REFERENCES operators(operator_id),
    fault_code     TEXT NOT NULL,
    severity       TEXT NOT NULL,
    event_ts       TIMESTAMPTZ NOT NULL,
    resolved_at    TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_faults_operator_ts ON faults (operator_id, event_ts DESC);
CREATE INDEX idx_faults_open ON faults (operator_id) WHERE resolved_at IS NULL;

-- ─── INVOICES ───────────────────────────────────────────────────────────────
CREATE TABLE invoices (
    invoice_id     TEXT PRIMARY KEY,
    session_id     TEXT UNIQUE NOT NULL REFERENCES sessions(session_id),
    operator_id    TEXT NOT NULL REFERENCES operators(operator_id),
    energy_wh      BIGINT NOT NULL,
    tariff_per_kwh INTEGER NOT NULL,
    session_fee    INTEGER NOT NULL,
    amount_minor   BIGINT NOT NULL,
    currency       CHAR(3) NOT NULL,
    invoice_date   DATE NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_invoices_operator_date ON invoices (operator_id, invoice_date);

-- ─── POLL STATE ─────────────────────────────────────────────────────────────
CREATE TABLE site_poll_state (
    site_id             TEXT PRIMARY KEY REFERENCES sites(site_id),
    last_polled_at      TIMESTAMPTZ,
    last_event_ts       TIMESTAMPTZ,
    cursor              TEXT,
    consecutive_errors  INTEGER NOT NULL DEFAULT 0,
    next_poll_at        TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ─── SEED PILOT DATA ────────────────────────────────────────────────────────
INSERT INTO operators (operator_id, name, country, tariff_per_kwh_minor, session_fee_minor, currency)
VALUES
    ('acme-charge', 'Acme Charge Pvt Ltd', 'IN', 850, 5000, 'INR'),
    ('stadtwerke-ev', 'Stadtwerke EV GmbH', 'DE', 45, 200, 'EUR')
ON CONFLICT DO NOTHING;

INSERT INTO sites (site_id, operator_id, name, address, country)
VALUES
    ('S-IN-0007', 'acme-charge', 'Sector 18 Metro', 'Sector 18 Metro Station, Gate 3, Noida, UP 201301', 'IN'),
    ('S-IN-0012', 'acme-charge', 'Phoenix Mall', 'Phoenix Marketcity, Whitefield Rd, Mahadevapura, Bengaluru, KA 560048', 'IN'),
    ('S-DE-0003', 'stadtwerke-ev', 'Hauptbahnhof P2', 'Ladestation Hauptbahnhof, Arnulf-Klett-Platz 2, 70173 Stuttgart', 'DE')
ON CONFLICT DO NOTHING;

INSERT INTO chargers (charger_id, site_id, operator_id, model, max_kw)
VALUES
    ('C-IN-0007-A', 'S-IN-0007', 'acme-charge', 'DeltaDC-60', 60),
    ('C-IN-0007-B', 'S-IN-0007', 'acme-charge', 'DeltaDC-60', 60),
    ('C-IN-0012-A', 'S-IN-0012', 'acme-charge', 'ABB-Terra-AC', 22),
    ('C-DE-0003-A', 'S-DE-0003', 'stadtwerke-ev', 'Alpitronic-HYC', 150)
ON CONFLICT DO NOTHING;

INSERT INTO connectors (charger_id, connector_id, operator_id)
VALUES
    ('C-IN-0007-A', '1', 'acme-charge'),
    ('C-IN-0007-A', '2', 'acme-charge'),
    ('C-IN-0007-B', '1', 'acme-charge'),
    ('C-IN-0012-A', '1', 'acme-charge'),
    ('C-DE-0003-A', '1', 'stadtwerke-ev'),
    ('C-DE-0003-A', '2', 'stadtwerke-ev')
ON CONFLICT DO NOTHING;

INSERT INTO site_poll_state (site_id, next_poll_at)
VALUES
    ('S-IN-0007', now()),
    ('S-IN-0012', now()),
    ('S-DE-0003', now())
ON CONFLICT DO NOTHING;
