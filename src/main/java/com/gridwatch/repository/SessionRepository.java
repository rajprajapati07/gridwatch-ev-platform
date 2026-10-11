package com.gridwatch.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;

@Slf4j
@Repository
@RequiredArgsConstructor
public class SessionRepository {

    private final JdbcTemplate jdbcTemplate;

    /**
     * Upsert session start.
     *
     * ON CONFLICT (session_id): if session.start arrives twice (at-least-once delivery),
     * update start_meter_wh only if the new value differs (idempotent).
     *
     * If session.stop arrived BEFORE session.start (out-of-order), the row already exists
     * with stop data; this upsert fills in the start fields without touching stop fields.
     */
    public void upsertSessionStart(
            String sessionId,
            String chargerId,
            String connectorId,
            String operatorId,
            OffsetDateTime startTs,
            Long startMeterWh) {

        jdbcTemplate.update("""
            INSERT INTO sessions
                (session_id, charger_id, connector_id, operator_id,
                 start_ts, start_meter_wh, status)
            VALUES (?, ?, ?, ?, ?, ?, 'active')
            ON CONFLICT (session_id) DO UPDATE
               SET start_ts       = EXCLUDED.start_ts,
                   start_meter_wh = EXCLUDED.start_meter_wh,
                   status         = CASE
                                      WHEN sessions.status = 'ended' THEN 'ended'
                                      ELSE 'active'
                                    END
            """,
            sessionId, chargerId, connectorId, operatorId, startTs, startMeterWh
        );
    }

    /**
     * Upsert session stop.
     *
     * Key invariants:
     * 1. ON CONFLICT (session_id) DO UPDATE — idempotent on duplicate stop events
     * 2. energy_wh is pre-calculated (null if meter_reset = true)
     * 3. meter_reset flag triggers ops alert, prevents billing
     *
     * If stop arrives before start (out-of-order), the INSERT creates the row;
     * start data will be filled in when session.start arrives.
     */
    public void upsertSessionStop(
            String sessionId,
            OffsetDateTime stopTs,
            Long stopMeterWh,
            Long energyWh,
            boolean meterReset) {

        jdbcTemplate.update("""
            INSERT INTO sessions
                (session_id, stop_ts, stop_meter_wh, energy_wh, meter_reset, status)
            VALUES (?, ?, ?, ?, ?, 'ended')
            ON CONFLICT (session_id) DO UPDATE
               SET stop_ts      = EXCLUDED.stop_ts,
                   stop_meter_wh = EXCLUDED.stop_meter_wh,
                   energy_wh    = EXCLUDED.energy_wh,
                   meter_reset  = EXCLUDED.meter_reset,
                   status       = 'ended'
            """,
            sessionId, stopTs, stopMeterWh, energyWh, meterReset
        );
    }

    /**
     * Count active (in-progress) sessions for an operator.
     */
    public long countActiveSessions(String operatorId) {
        Long count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM sessions WHERE operator_id = ? AND status = 'active'",
            Long.class, operatorId
        );
        return count != null ? count : 0L;
    }
}
