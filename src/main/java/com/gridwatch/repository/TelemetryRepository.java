package com.gridwatch.repository;

import com.gridwatch.model.CsmsEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Raw telemetry writes to the TimescaleDB hypertable.
 *
 * Uses JdbcTemplate directly (not JPA) for two reasons:
 * 1. TimescaleDB hypertables work best with batched COPY or plain JDBC — not ORM magic
 * 2. The INSERT ... ON CONFLICT DO NOTHING pattern is cleaner without @Query tricks
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class TelemetryRepository {

    private final JdbcTemplate jdbcTemplate;

    /**
     * Inserts a telemetry row.
     * ON CONFLICT (event_key) DO NOTHING ensures idempotency — duplicate events from
     * at-least-once webhook delivery or poll overlap are silently discarded.
     */
    public void insertTelemetry(CsmsEvent event) {
        try {
            jdbcTemplate.update("""
                INSERT INTO telemetry
                    (event_key, charger_id, connector_id, operator_id, event_type,
                     event_ts, ingested_at, source,
                     status, energy_register_wh, power_w,
                     fault_code, fault_severity,
                     session_id, session_event)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (event_key) DO NOTHING
                """,
                event.getEventKey(),
                event.getChargerId(),
                event.getConnectorId(),
                event.getOperatorId(),
                event.getType().name(),
                event.getEventTs(),
                event.getIngestedAt(),
                event.getSource().name(),
                event.getStatus(),
                event.getEnergyRegisterWh(),
                event.getPowerW(),
                event.getFaultCode(),
                event.getFaultSeverity(),
                event.getSessionId(),
                event.getSessionEvent()
            );
        } catch (Exception e) {
            log.error("Failed to insert telemetry event_key={}: {}", event.getEventKey(), e.getMessage());
            throw e;
        }
    }
}
