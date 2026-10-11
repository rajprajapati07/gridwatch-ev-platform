package com.gridwatch.service;

import com.gridwatch.model.CsmsEvent;
import com.gridwatch.repository.ConnectorRepository;
import com.gridwatch.repository.FaultRepository;
import com.gridwatch.repository.SessionRepository;
import com.gridwatch.repository.TelemetryRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for StatusConsumer — verifying all correctness invariants.
 *
 * Tests the four critical failure modes from the brief:
 * 1. Duplicate session.stop (at-least-once delivery) → no double billing
 * 2. Out-of-order STATUS events → newest wins by event_ts
 * 3. Meter reset (stop_meter_wh < start_meter_wh) → flagged, energy_wh = null
 * 4. Duplicate events from hot-path dedup → skipped efficiently
 */
@ExtendWith(MockitoExtension.class)
class StatusConsumerTest {

    @Mock ConnectorRepository connectorRepository;
    @Mock TelemetryRepository telemetryRepository;
    @Mock FaultRepository faultRepository;
    @Mock SessionRepository sessionRepository;
    @Mock StringRedisTemplate redisTemplate;
    @Mock ValueOperations<String, String> valueOps;
    @Mock SimpMessagingTemplate messagingTemplate;
    @Mock Acknowledgment ack;

    private StatusConsumer consumer;

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        consumer = new StatusConsumer(
            connectorRepository, telemetryRepository, faultRepository,
            sessionRepository, redisTemplate, messagingTemplate, new SimpleMeterRegistry()
        );
    }

    // ─── FAILURE MODE 1: OUT-OF-ORDER STATUS EVENTS ───────────────────────────

    @Test
    @DisplayName("FAILURE MODE: Stale STATUS event does NOT overwrite newer status")
    void consume_staleStatusEvent_doesNotUpdateRedisOrPushWebSocket() {
        // Setup: Redis SETNX returns true (not a duplicate at event_key level)
        when(valueOps.setIfAbsent(anyString(), anyString(), anyLong(), any())).thenReturn(true);

        // But the DB compare-and-swap returns 0 — the incoming event is older than stored
        when(connectorRepository.updateStatusIfNewer(any(), any(), any(), any())).thenReturn(0);

        CsmsEvent staleEvent = buildStatusEvent("CHR-001", "C1", "Available",
            OffsetDateTime.of(2024, 1, 15, 10, 0, 0, 0, ZoneOffset.UTC)); // older

        consumer.consume(staleEvent, ack);

        // Redis must NOT be updated with the stale status
        verify(valueOps, never()).set(contains("status:"), anyString(), anyLong(), any());
        // WebSocket must NOT push a stale diff
        verify(messagingTemplate, never()).convertAndSend(anyString(), any(Object.class));
        // Telemetry IS written (for historical record)
        verify(telemetryRepository).insertTelemetry(staleEvent);
        // Kafka offset IS committed
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Newer STATUS event DOES update Redis and push WebSocket diff")
    void consume_newerStatusEvent_updatesRedisAndPushes() {
        when(valueOps.setIfAbsent(anyString(), anyString(), anyLong(), any())).thenReturn(true);
        when(connectorRepository.updateStatusIfNewer(any(), any(), any(), any())).thenReturn(1); // updated

        CsmsEvent event = buildStatusEvent("CHR-001", "C1", "Charging",
            OffsetDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneOffset.UTC));

        consumer.consume(event, ack);

        verify(valueOps).set(eq("status:op-india:CHR-001:C1"), anyString(), anyLong(), any());
        verify(messagingTemplate).convertAndSend(eq("/topic/status/op-india"), any(Object.class));
        verify(ack).acknowledge();
    }

    // ─── FAILURE MODE 2: DUPLICATE EVENTS (hot-path dedup) ───────────────────

    @Test
    @DisplayName("FAILURE MODE: Duplicate event (same event_key) is skipped entirely")
    void consume_duplicateEventKey_skipped_noDbWrite() {
        // SETNX returns false → key already exists → duplicate
        when(valueOps.setIfAbsent(anyString(), anyString(), anyLong(), any())).thenReturn(false);

        CsmsEvent duplicate = buildStatusEvent("CHR-001", "C1", "Charging",
            OffsetDateTime.now(ZoneOffset.UTC));

        consumer.consume(duplicate, ack);

        // No DB writes for duplicates
        verify(connectorRepository, never()).updateStatusIfNewer(any(), any(), any(), any());
        verify(telemetryRepository, never()).insertTelemetry(any());
        // Still acks — don't replay duplicates
        verify(ack).acknowledge();
    }

    // ─── FAILURE MODE 3: METER RESET ─────────────────────────────────────────

    @Test
    @DisplayName("FAILURE MODE: Meter reset (stop < start) → energy_wh=null, meter_reset=true")
    void consume_meterReset_flaggedNotBilled() {
        when(valueOps.setIfAbsent(anyString(), anyString(), anyLong(), any())).thenReturn(true);

        CsmsEvent stopEvent = new CsmsEvent();
        stopEvent.setEventKey("evt-meter-reset-001");
        stopEvent.setChargerId("CHR-001");
        stopEvent.setConnectorId("C1");
        stopEvent.setOperatorId("op-india");
        stopEvent.setType(CsmsEvent.EventType.SESSION);
        stopEvent.setSessionId("session-99");
        stopEvent.setSessionEvent("session.stop");
        stopEvent.setStartMeterWh(50000L);
        stopEvent.setStopMeterWh(100L);   // ← METER RESET: stop < start
        stopEvent.setEventTs(OffsetDateTime.now(ZoneOffset.UTC));
        stopEvent.setIngestedAt(OffsetDateTime.now(ZoneOffset.UTC));
        stopEvent.setSource(CsmsEvent.IngestionSource.WEBHOOK);

        consumer.consume(stopEvent, ack);

        // Verify upsertSessionStop is called with meterReset=true and energyWh=null
        verify(sessionRepository).upsertSessionStop(
            eq("session-99"),
            any(),
            eq(100L),         // stopMeterWh
            isNull(),         // energyWh must be null (NOT billed)
            eq(true)          // meterReset must be true
        );
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Normal session stop: energy_wh = stop - start, meter_reset = false")
    void consume_normalSessionStop_energyCalculated() {
        when(valueOps.setIfAbsent(anyString(), anyString(), anyLong(), any())).thenReturn(true);

        CsmsEvent stopEvent = new CsmsEvent();
        stopEvent.setEventKey("evt-session-stop-001");
        stopEvent.setChargerId("CHR-001");
        stopEvent.setConnectorId("C1");
        stopEvent.setOperatorId("op-india");
        stopEvent.setType(CsmsEvent.EventType.SESSION);
        stopEvent.setSessionId("session-100");
        stopEvent.setSessionEvent("session.stop");
        stopEvent.setStartMeterWh(50000L);
        stopEvent.setStopMeterWh(57400L);  // 7.4 kWh delivered
        stopEvent.setEventTs(OffsetDateTime.now(ZoneOffset.UTC));
        stopEvent.setIngestedAt(OffsetDateTime.now(ZoneOffset.UTC));
        stopEvent.setSource(CsmsEvent.IngestionSource.WEBHOOK);

        consumer.consume(stopEvent, ack);

        verify(sessionRepository).upsertSessionStop(
            eq("session-100"),
            any(),
            eq(57400L),
            eq(7400L),    // energy = 57400 - 50000 = 7400 Wh
            eq(false)     // not a meter reset
        );
    }

    // ─── FAILURE MODE 4: DUPLICATE SESSION.STOP (double billing) ─────────────

    @Test
    @DisplayName("FAILURE MODE: Two identical session.stop events → upsertSessionStop called once (Redis dedup)")
    void consume_duplicateSessionStop_deduplicated() {
        // First: SETNX succeeds → process
        when(valueOps.setIfAbsent(anyString(), anyString(), anyLong(), any()))
            .thenReturn(true)    // first delivery
            .thenReturn(false);  // second delivery — duplicate

        CsmsEvent stopEvent = new CsmsEvent();
        stopEvent.setEventKey("evt-stop-dedup-001");
        stopEvent.setChargerId("CHR-001");
        stopEvent.setConnectorId("C1");
        stopEvent.setOperatorId("op-india");
        stopEvent.setType(CsmsEvent.EventType.SESSION);
        stopEvent.setSessionId("session-777");
        stopEvent.setSessionEvent("session.stop");
        stopEvent.setStartMeterWh(10000L);
        stopEvent.setStopMeterWh(17000L);
        stopEvent.setEventTs(OffsetDateTime.now(ZoneOffset.UTC));
        stopEvent.setIngestedAt(OffsetDateTime.now(ZoneOffset.UTC));
        stopEvent.setSource(CsmsEvent.IngestionSource.WEBHOOK);

        consumer.consume(stopEvent, ack);  // first delivery
        consumer.consume(stopEvent, ack);  // duplicate — same event_key

        // Session repo must only be called ONCE (not twice = not double-billed)
        verify(sessionRepository, times(1)).upsertSessionStop(any(), any(), any(), any(), anyBoolean());
    }

    // ─── FAULT EVENT HANDLING ─────────────────────────────────────────────────

    @Test
    @DisplayName("FAULT event: pushes WebSocket alert and inserts idempotently")
    void consume_faultEvent_pushesAlertAndInserts() {
        when(valueOps.setIfAbsent(anyString(), anyString(), anyLong(), any())).thenReturn(true);

        CsmsEvent fault = new CsmsEvent();
        fault.setEventKey("evt-fault-001");
        fault.setChargerId("CHR-001");
        fault.setConnectorId("C1");
        fault.setOperatorId("op-india");
        fault.setType(CsmsEvent.EventType.FAULT);
        fault.setFaultCode("E003");
        fault.setFaultSeverity("Critical");
        fault.setEventTs(OffsetDateTime.now(ZoneOffset.UTC));
        fault.setIngestedAt(OffsetDateTime.now(ZoneOffset.UTC));
        fault.setSource(CsmsEvent.IngestionSource.WEBHOOK);

        consumer.consume(fault, ack);

        verify(faultRepository).insertFaultIdempotent(
            eq("evt-fault-001"), eq("CHR-001"), eq("C1"), eq("op-india"),
            eq("E003"), eq("Critical"), any()
        );
        verify(messagingTemplate).convertAndSend(eq("/topic/faults/op-india"), any(Object.class));
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Exception during processing → Kafka NOT acked → event replayed")
    void consume_exceptionDuringProcessing_kafkaNotAcked() {
        when(valueOps.setIfAbsent(anyString(), anyString(), anyLong(), any())).thenReturn(true);
        when(connectorRepository.updateStatusIfNewer(any(), any(), any(), any()))
            .thenThrow(new RuntimeException("DB connection lost"));

        CsmsEvent event = buildStatusEvent("CHR-001", "C1", "Charging",
            OffsetDateTime.now(ZoneOffset.UTC));

        consumer.consume(event, ack);

        // Kafka offset must NOT be committed — event will be redelivered
        verify(ack, never()).acknowledge();
    }

    // ─── HELPERS ─────────────────────────────────────────────────────────────

    private CsmsEvent buildStatusEvent(String chargerId, String connectorId,
                                        String status, OffsetDateTime eventTs) {
        CsmsEvent event = new CsmsEvent();
        event.setEventKey("evt-" + chargerId + "-" + connectorId + "-" + eventTs.toEpochSecond());
        event.setChargerId(chargerId);
        event.setConnectorId(connectorId);
        event.setOperatorId("op-india");
        event.setType(CsmsEvent.EventType.STATUS);
        event.setStatus(status);
        event.setEventTs(eventTs);
        event.setIngestedAt(OffsetDateTime.now(ZoneOffset.UTC));
        event.setSource(CsmsEvent.IngestionSource.WEBHOOK);
        return event;
    }
}
