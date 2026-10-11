package com.gridwatch.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
@RequiredArgsConstructor
public class FaultRepository {

    private final JdbcTemplate jdbcTemplate;

    /**
     * Idempotent fault insert.
     * ON CONFLICT (event_key) DO NOTHING — duplicate FAULT events from at-least-once
     * webhook delivery are silently discarded. The UNIQUE constraint on event_key is
     * defined in V1__init_schema.sql.
     */
    public void insertFaultIdempotent(
            String eventKey,
            String chargerId,
            String connectorId,
            String operatorId,
            String faultCode,
            String faultSeverity,
            OffsetDateTime eventTs) {

        jdbcTemplate.update("""
            INSERT INTO faults
                (event_key, charger_id, connector_id, operator_id,
                 fault_code, severity, event_ts, resolved)
            VALUES (?, ?, ?, ?, ?, ?, ?, false)
            ON CONFLICT (event_key) DO NOTHING
            """,
            eventKey, chargerId, connectorId, operatorId,
            faultCode, faultSeverity, eventTs
        );
    }

    /**
     * Returns open (unresolved) faults for an operator, ordered by severity then time.
     */
    public List<Map<String, Object>> findOpenFaults(String operatorId) {
        return jdbcTemplate.queryForList("""
            SELECT f.event_key, f.charger_id, f.connector_id,
                   f.fault_code, f.severity, f.event_ts,
                   c.name AS charger_name,
                   s.name AS site_name
              FROM faults f
              JOIN chargers c ON c.charger_id = f.charger_id
              JOIN sites s ON s.site_id = c.site_id
             WHERE f.operator_id = ?
               AND f.resolved = false
             ORDER BY
               CASE f.severity WHEN 'Critical' THEN 1 WHEN 'Warning' THEN 2 ELSE 3 END,
               f.event_ts DESC
             LIMIT 200
            """, operatorId);
    }
}
