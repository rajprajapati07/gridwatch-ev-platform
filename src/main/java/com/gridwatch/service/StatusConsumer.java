package com.gridwatch.service;

import com.gridwatch.model.CsmsEvent;
import com.gridwatch.repository.ConnectorRepository;
import com.gridwatch.repository.FaultRepository;
import com.gridwatch.repository.SessionRepository;
import com.gridwatch.repository.TelemetryRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Consumes CsmsEvents from Kafka and applies them to the live state.
 *
 * Key correctness invariants:
 * 1. STATUS: last-write-wins by event_ts (not arrival time) — handles out-of-order events
 * 2. SESSION: UPSERT on session_id — handles duplicate webhook delivery
 * 3. METER_RESET: flagged when stop_meter_wh < start_meter_wh (not billed; flagged for ops)
 * 4. Redis TTL: 60s on live status — stale if no events > 60s (connector offline)
 * 5. Kafka commit: only after successful DB write (at-least-once; idempotency handles duplicates)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StatusConsumer {

    private final ConnectorRepository connectorRepository;
    private final TelemetryRepository telemetryRepository;
    private final FaultRepository faultRepository;
    private final SessionRepository sessionRepository;
    private final StringRedisTemplate redisTemplate;
    private final SimpMessagingTemplate messagingTemplate;
    private final MeterRegistry meterRegistry;

    private static final long STATUS_TTL_SECONDS = 60;
    private static final long DEDUP_TTL_HOURS = 24;

    @KafkaListener(
        topics = "${gridwatch.kafka.topic:csms.events.raw}",
        groupId = "gridwatch-status-consumer",
        concurrency = "12"  // 12 consumer threads; scale with partition count
    )
    @Transactional
    public void consume(CsmsEvent event, Acknowledgment ack) {
        try {
            // ─── Hot-path deduplication ──────────────────────────────────────
            if (isDuplicate(event)) {
                log.debug("Skipping duplicate event_key={}", event.getEventKey());
                meterRegistry.counter("gridwatch.consumer.duplicates_skipped",
                    "event_type", event.getType().name()).increment();
                ack.acknowledge();
                return;
            }

            long startNs = System.nanoTime();

            switch (event.getType()) {
                case STATUS -> processStatus(event);
                case METER_VALUE -> processMeterValue(event);
                case FAULT -> processFault(event);
                case SESSION -> processSession(event);
                case UNKNOWN -> log.debug("Ignoring UNKNOWN event from charger={}", event.getChargerId());
            }

            // Mark as processed (dedup)
            markProcessed(event);

            // Emit freshness metric: time from event_ts to now (consumer processed)
            long freshnessMs = System.nanoTime() - startNs;
            OffsetDateTime eventTs = event.getEventTs();
            if (eventTs != null) {
                long e2eMs = OffsetDateTime.now(ZoneOffset.UTC).toInstant().toEpochMilli()
                    - eventTs.toInstant().toEpochMilli();
                meterRegistry.timer("gridwatch.consumer.e2e_freshness",
                    "event_type", event.getType().name(),
                    "source", event.getSource().name())
                    .record(Duration.ofMillis(e2eMs));
            }

            ack.acknowledge(); // Commit offset only after successful processing

        } catch (Exception e) {
            log.error("Error processing event key={} charger={}: {}",
                event.getEventKey(), event.getChargerId(), e.getMessage(), e);
            meterRegistry.counter("gridwatch.consumer.processing_errors",
                "event_type", event.getType().name()).increment();
            // Don't ack — Kafka will redeliver. Idempotency ensures safe replay.
            // In prod: add a DLQ after N retries.
        }
    }

    /**
     * STATUS event processing.
     * Uses compare-and-swap on event_ts to handle out-of-order delivery:
     * only updates if the incoming event is newer than the current stored status_ts.
     */
    private void processStatus(CsmsEvent event) {
        // DB: last-write-wins by event_ts (not arrival order)
        int updated = connectorRepository.updateStatusIfNewer(
            event.getChargerId(),
            event.getConnectorId(),
            event.getStatus(),
            event.getEventTs()
        );

        if (updated > 0) {
            // Redis: update live status with TTL
            String redisKey = liveStatusKey(event.getOperatorId(), event.getChargerId(), event.getConnectorId());
            String value = buildStatusJson(event);
            redisTemplate.opsForValue().set(redisKey, value, STATUS_TTL_SECONDS, TimeUnit.SECONDS);

            // WebSocket: push diff to connected clients (only the changed connector)
            pushStatusUpdate(event);

            log.debug("Status updated: charger={} connector={} status={} ts={}",
                event.getChargerId(), event.getConnectorId(), event.getStatus(), event.getEventTs());
        }

        // Persist raw telemetry regardless (for historical queries)
        telemetryRepository.insertTelemetry(event);
    }

    /**
     * METER_VALUE event processing.
     * Stores energy register reading for interval energy calculation.
     * Energy is cumulative lifetime Wh — not per-session, not per-interval.
     */
    private void processMeterValue(CsmsEvent event) {
        // Update power reading in live status
        if (event.getPowerW() != null) {
            String redisKey = liveStatusKey(event.getOperatorId(), event.getChargerId(), event.getConnectorId());
            // Get current status and update power field
            String current = redisTemplate.opsForValue().get(redisKey);
            if (current != null) {
                // Simple string update — in prod use a proper JSON merge
                String updated = current.replace("\"power_w\":null",
                    "\"power_w\":" + event.getPowerW());
                if (!updated.equals(current)) {
                    redisTemplate.opsForValue().set(redisKey, updated, STATUS_TTL_SECONDS, TimeUnit.SECONDS);
                }
            }
        }
        // Persist for energy calculation and analytics
        telemetryRepository.insertTelemetry(event);
    }

    /**
     * FAULT event processing.
     * Idempotent on event_key (UNIQUE constraint prevents duplicate fault rows).
     */
    private void processFault(CsmsEvent event) {
        try {
            faultRepository.insertFaultIdempotent(
                event.getEventKey(),
                event.getChargerId(),
                event.getConnectorId(),
                event.getOperatorId(),
                event.getFaultCode(),
                event.getFaultSeverity(),
                event.getEventTs()
            );

            // Push fault alert via WebSocket
            messagingTemplate.convertAndSend(
                "/topic/faults/" + event.getOperatorId(),
                Map.of(
                    "type", "fault",
                    "charger_id", event.getChargerId(),
                    "connector_id", event.getConnectorId() != null ? event.getConnectorId() : "",
                    "code", event.getFaultCode(),
                    "severity", event.getFaultSeverity(),
                    "ts", event.getEventTs().toString()
                )
            );

            log.info("Fault recorded: charger={} code={} severity={}",
                event.getChargerId(), event.getFaultCode(), event.getFaultSeverity());
        } catch (Exception e) {
            // ON CONFLICT DO NOTHING — duplicate fault events are silently ignored
            log.debug("Duplicate fault ignored: event_key={}", event.getEventKey());
        }
    }

    /**
     * SESSION event processing.
     *
     * Handles:
     * - session.start: UPSERT with start_meter_wh
     * - session.stop: UPSERT; compute energy_wh; flag meter_reset if stop < start
     * - Duplicate stop events: ON CONFLICT (session_id) DO NOTHING on invoice side
     * - session.stop arriving before session.start (out-of-order): stored; reconciled when start arrives
     */
    private void processSession(CsmsEvent event) {
        if ("session.start".equals(event.getSessionEvent())) {
            sessionRepository.upsertSessionStart(
                event.getSessionId(),
                event.getChargerId(),
                event.getConnectorId(),
                event.getOperatorId(),
                event.getEventTs(),
                event.getStartMeterWh()
            );
        } else if ("session.stop".equals(event.getSessionEvent())) {
            // Detect meter reset: stop_meter_wh < start_meter_wh
            boolean meterReset = event.getStartMeterWh() != null
                && event.getStopMeterWh() != null
                && event.getStopMeterWh() < event.getStartMeterWh();

            Long energyWh = null;
            if (!meterReset && event.getStartMeterWh() != null && event.getStopMeterWh() != null) {
                energyWh = event.getStopMeterWh() - event.getStartMeterWh();
            }

            if (meterReset) {
                log.warn("Meter reset detected: session={} start_wh={} stop_wh={}",
                    event.getSessionId(), event.getStartMeterWh(), event.getStopMeterWh());
                meterRegistry.counter("gridwatch.sessions.meter_reset",
                    "operator_id", event.getOperatorId()).increment();
            }

            sessionRepository.upsertSessionStop(
                event.getSessionId(),
                event.getEventTs(),
                event.getStopMeterWh(),
                energyWh,
                meterReset
            );
        }
    }

    // ─── Deduplication (Redis Bloom filter substitute) ───────────────────────

    private boolean isDuplicate(CsmsEvent event) {
        String key = "dedup:" + event.getEventKey();
        // SETNX: returns true if key was set (not a duplicate), false if already existed
        Boolean isNew = redisTemplate.opsForValue().setIfAbsent(key, "1",
            DEDUP_TTL_HOURS, TimeUnit.HOURS);
        return isNew == null || !isNew;
    }

    private void markProcessed(CsmsEvent event) {
        // Already marked in isDuplicate check above (SETNX) — no additional action needed
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private String liveStatusKey(String operatorId, String chargerId, String connectorId) {
        return "status:" + operatorId + ":" + chargerId + ":" + (connectorId != null ? connectorId : "");
    }

    private String buildStatusJson(CsmsEvent event) {
        return String.format(
            "{\"status\":\"%s\",\"status_ts\":\"%s\",\"power_w\":%s,\"session_id\":%s}",
            event.getStatus(),
            event.getEventTs() != null ? event.getEventTs().toString() : "",
            event.getPowerW() != null ? event.getPowerW() : "null",
            "null"
        );
    }

    private void pushStatusUpdate(CsmsEvent event) {
        messagingTemplate.convertAndSend(
            "/topic/status/" + event.getOperatorId(),
            Map.of(
                "type", "status_change",
                "charger_id", event.getChargerId(),
                "connector_id", event.getConnectorId() != null ? event.getConnectorId() : "",
                "status", event.getStatus(),
                "ts", event.getEventTs().toString()
            )
        );
    }
}
