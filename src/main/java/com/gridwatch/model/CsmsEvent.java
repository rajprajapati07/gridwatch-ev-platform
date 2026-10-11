package com.gridwatch.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

/**
 * Normalised internal representation of a CSMS event.
 * Events from both the poll API and webhook are normalised into this shape
 * before being published to Kafka.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CsmsEvent {

    /** Deterministic deduplication key: SHA256(charger_id+connector_id+type+ts_utc+type_fields) */
    private String eventKey;

    private String chargerId;
    private String connectorId;
    private String operatorId;

    /** Normalised event type */
    private EventType type;

    /** Event timestamp normalised to UTC (from charger local time with offset) */
    private OffsetDateTime eventTs;

    /** Ingestion timestamp (wall clock when we received it) */
    private OffsetDateTime ingestedAt;

    // --- Type-specific fields (null if not applicable) ---

    // STATUS events
    private String status;

    // METER_VALUE events
    private Long energyRegisterWh;   // cumulative lifetime register
    private Integer powerW;

    // FAULT events
    private String faultCode;
    private String faultSeverity;

    // SESSION events
    private String sessionId;
    private String sessionEvent;     // "session.start" or "session.stop"
    private Long startMeterWh;
    private Long stopMeterWh;

    /** Source of this event (for observability) */
    private IngestionSource source;

    public enum EventType {
        STATUS, METER_VALUE, FAULT, SESSION, UNKNOWN
    }

    public enum IngestionSource {
        WEBHOOK, POLL
    }
}
