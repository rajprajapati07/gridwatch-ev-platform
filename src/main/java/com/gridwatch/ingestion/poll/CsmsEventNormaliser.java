package com.gridwatch.ingestion.poll;

import com.gridwatch.model.CsmsEvent;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;

/**
 * Normalises raw CSMS event maps (from poll API or webhook) into CsmsEvent objects.
 *
 * Key responsibilities:
 * 1. Parse and normalise timestamps (charger local tz with offset → UTC OffsetDateTime)
 * 2. Validate required fields — skip malformed events without crashing the batch
 * 3. Assign deterministic event_key for deduplication
 * 4. Map raw type strings to EventType enum
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CsmsEventNormaliser {

    private final MeterRegistry meterRegistry;

    /**
     * Attempt to normalise a raw event map from the CSMS.
     * Returns empty Optional for malformed events — caller skips and continues.
     */
    public Optional<CsmsEvent> normalise(Map<String, Object> raw,
                                          String operatorId,
                                          CsmsEvent.IngestionSource source) {
        try {
            return Optional.of(doNormalise(raw, operatorId, source));
        } catch (MalformedEventException e) {
            // Log and meter the skip — do NOT propagate to caller
            log.warn("Skipping malformed event: {} | raw={}", e.getMessage(), raw);
            meterRegistry.counter("gridwatch.ingestion.events.malformed",
                "operator_id", operatorId, "source", source.name()).increment();
            return Optional.empty();
        }
    }

    private CsmsEvent doNormalise(Map<String, Object> raw,
                                   String operatorId,
                                   CsmsEvent.IngestionSource source) throws MalformedEventException {

        String typeStr = getString(raw, "type")
            .orElseThrow(() -> new MalformedEventException("missing 'type' field"));

        String chargerId = getString(raw, "charger_id")
            .orElseThrow(() -> new MalformedEventException("missing 'charger_id' in type=" + typeStr));

        // connector_id is required for most types but absent for some charger-level events
        String connectorId = getString(raw, "connector_id").orElse(null);

        // Parse timestamp — REQUIRED; charger local timezone with offset
        String tsRaw = getString(raw, "ts")
            .orElseThrow(() -> new MalformedEventException("missing 'ts' in charger=" + chargerId));

        OffsetDateTime eventTs = parseTimestamp(tsRaw, chargerId);

        CsmsEvent.EventType eventType = mapEventType(typeStr);

        CsmsEvent event = new CsmsEvent();
        event.setChargerId(chargerId);
        event.setConnectorId(connectorId);
        event.setOperatorId(operatorId);
        event.setType(eventType);
        event.setEventTs(eventTs);
        event.setIngestedAt(OffsetDateTime.now());
        event.setSource(source);

        // Populate type-specific fields
        switch (eventType) {
            case STATUS -> {
                String status = getString(raw, "status")
                    .orElseThrow(() -> new MalformedEventException("STATUS event missing 'status' field"));
                event.setStatus(status);
            }
            case METER_VALUE -> {
                // energy_register_wh may be null (malformed) — treat as skip
                Long energyWh = getLong(raw, "energy_register_wh")
                    .orElseThrow(() -> new MalformedEventException(
                        "METER_VALUE event has null 'energy_register_wh' in charger=" + chargerId));
                event.setEnergyRegisterWh(energyWh);
                event.setPowerW(getInt(raw, "power_w").orElse(null));
            }
            case FAULT -> {
                event.setFaultCode(getString(raw, "code").orElse("UNKNOWN"));
                event.setFaultSeverity(getString(raw, "severity").orElse("unknown"));
            }
            case SESSION -> {
                String sessionEvent = getString(raw, "event")
                    .orElseThrow(() -> new MalformedEventException("SESSION event missing 'event' subtype"));
                event.setSessionEvent(sessionEvent);
                event.setSessionId(getString(raw, "session_id")
                    .orElseThrow(() -> new MalformedEventException("SESSION event missing 'session_id'")));
                event.setStartMeterWh(getLong(raw, "start_meter_wh").orElse(null));
                if ("session.stop".equals(sessionEvent)) {
                    event.setStopMeterWh(getLong(raw, "stop_meter_wh").orElse(null));
                }
            }
            case UNKNOWN -> log.debug("Unknown event type '{}' from charger={}", typeStr, chargerId);
        }

        event.setEventKey(computeEventKey(event));
        return event;
    }

    /**
     * Parse ISO 8601 timestamp with UTC offset.
     * The CSMS sends timestamps in charger local timezone (e.g. +05:30 for India, +02:00 for Germany DST).
     * OffsetDateTime parses these correctly; we store as timestamptz (UTC equivalent).
     */
    private OffsetDateTime parseTimestamp(String ts, String chargerId) throws MalformedEventException {
        try {
            return OffsetDateTime.parse(ts); // ISO 8601 with offset — handles +05:30, +02:00, Z
        } catch (DateTimeParseException e) {
            throw new MalformedEventException(
                "unparseable timestamp '" + ts + "' in charger=" + chargerId);
        }
    }

    /**
     * Deterministic deduplication key.
     * Same event from both webhook and poll path produces the same key.
     */
    private String computeEventKey(CsmsEvent event) {
        String raw = String.join("|",
            nullSafe(event.getChargerId()),
            nullSafe(event.getConnectorId()),
            nullSafe(event.getType() != null ? event.getType().name() : null),
            nullSafe(event.getEventTs() != null ? event.getEventTs().toString() : null),
            nullSafe(event.getSessionId()),
            nullSafe(event.getStatus()),
            nullSafe(event.getFaultCode())
        );
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(raw.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash).substring(0, 32); // first 128 bits is enough
        } catch (Exception e) {
            return raw.hashCode() + ""; // fallback (shouldn't happen)
        }
    }

    private CsmsEvent.EventType mapEventType(String typeStr) {
        return switch (typeStr.toLowerCase()) {
            case "status" -> CsmsEvent.EventType.STATUS;
            case "meter_value" -> CsmsEvent.EventType.METER_VALUE;
            case "fault" -> CsmsEvent.EventType.FAULT;
            case "session" -> CsmsEvent.EventType.SESSION;
            default -> CsmsEvent.EventType.UNKNOWN;
        };
    }

    // ─── Field extraction helpers ────────────────────────────────────────────

    private Optional<String> getString(Map<String, Object> map, String key) {
        Object val = map.get(key);
        if (val == null) return Optional.empty();
        String s = val.toString().trim();
        return s.isEmpty() ? Optional.empty() : Optional.of(s);
    }

    private Optional<Long> getLong(Map<String, Object> map, String key) {
        Object val = map.get(key);
        if (val == null) return Optional.empty();
        try {
            return Optional.of(Long.parseLong(val.toString()));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private Optional<Integer> getInt(Map<String, Object> map, String key) {
        Object val = map.get(key);
        if (val == null) return Optional.empty();
        try {
            return Optional.of(Integer.parseInt(val.toString()));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private String nullSafe(String s) {
        return s == null ? "" : s;
    }

    public static class MalformedEventException extends Exception {
        public MalformedEventException(String message) {
            super(message);
        }
    }
}
