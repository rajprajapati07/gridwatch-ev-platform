package com.gridwatch.ingestion.poll;

import com.gridwatch.model.CsmsEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the normaliser's ability to handle malformed event batches without crashing.
 *
 * Brief failure mode: "A batch of 100 events where events[42] has a null energy_register_wh
 * and events[67] is missing connector_id entirely. The other 98 must be processed."
 */
class CsmsEventNormaliserTest {

    private CsmsEventNormaliser normaliser;

    @BeforeEach
    void setUp() {
        normaliser = new CsmsEventNormaliser(new SimpleMeterRegistry());
    }

    // ─── VALID EVENT NORMALISATION ─────────────────────────────────────────────

    @Test
    @DisplayName("STATUS event: normalises cleanly with UTC timestamp offset")
    void normalise_validStatusEvent_returnsEvent() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("charger_id", "CHR-001");
        raw.put("connector_id", "C1");
        raw.put("type", "status");
        raw.put("status", "Charging");
        raw.put("ts", "2024-01-15T10:30:00+05:30");  // IST — must normalise to UTC

        Optional<CsmsEvent> result = normaliser.normalise(raw, "op-india", CsmsEvent.IngestionSource.WEBHOOK);

        assertThat(result).isPresent();
        CsmsEvent event = result.get();
        assertThat(event.getStatus()).isEqualTo("Charging");
        assertThat(event.getChargerId()).isEqualTo("CHR-001");
        assertThat(event.getType()).isEqualTo(CsmsEvent.EventType.STATUS);
        // Verify timestamp was normalised — UTC offset = +05:30 means UTC = ts - 5:30
        assertThat(event.getEventTs().getOffset().getTotalSeconds()).isEqualTo(0); // UTC
        assertThat(event.getEventTs().getHour()).isEqualTo(5); // 10:30 IST = 05:00 UTC
        assertThat(event.getEventKey()).isNotBlank().hasSize(32); // 128-bit hex = 32 chars
    }

    @Test
    @DisplayName("STATUS event: German DST timestamp (+02:00) normalises correctly")
    void normalise_germanDstTimestamp_normalisesToUtc() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("charger_id", "CHR-DE-001");
        raw.put("connector_id", "C1");
        raw.put("type", "status");
        raw.put("status", "Available");
        raw.put("ts", "2024-07-15T14:00:00+02:00");  // CET summer (CEST)

        Optional<CsmsEvent> result = normaliser.normalise(raw, "op-germany", CsmsEvent.IngestionSource.POLL);

        assertThat(result).isPresent();
        assertThat(result.get().getEventTs().getHour()).isEqualTo(12); // 14:00+02 = 12:00 UTC
    }

    @Test
    @DisplayName("METER_VALUE event: normalises energy_register_wh")
    void normalise_validMeterValueEvent_returnsEvent() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("charger_id", "CHR-001");
        raw.put("connector_id", "C1");
        raw.put("type", "meter_value");
        raw.put("energy_register_wh", 123456L);
        raw.put("power_w", 7400);
        raw.put("ts", "2024-01-15T10:30:00Z");

        Optional<CsmsEvent> result = normaliser.normalise(raw, "op-india", CsmsEvent.IngestionSource.POLL);

        assertThat(result).isPresent();
        assertThat(result.get().getEnergyRegisterWh()).isEqualTo(123456L);
        assertThat(result.get().getPowerW()).isEqualTo(7400L);
    }

    // ─── FAILURE MODE: MALFORMED EVENTS ───────────────────────────────────────

    @Test
    @DisplayName("FAILURE MODE: null energy_register_wh in meter_value → skipped, not crash")
    void normalise_meterValue_nullEnergy_returnsEmpty() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("charger_id", "CHR-001");
        raw.put("connector_id", "C1");
        raw.put("type", "meter_value");
        raw.put("energy_register_wh", null);  // The failure mode from the brief
        raw.put("ts", "2024-01-15T10:30:00Z");

        Optional<CsmsEvent> result = normaliser.normalise(raw, "op-india", CsmsEvent.IngestionSource.POLL);

        // Must return empty, NOT throw
        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("FAILURE MODE: missing connector_id → skipped, not crash")
    void normalise_missingConnectorId_returnsEmpty() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("charger_id", "CHR-001");
        // connector_id intentionally absent
        raw.put("type", "status");
        raw.put("status", "Available");
        raw.put("ts", "2024-01-15T10:30:00Z");

        Optional<CsmsEvent> result = normaliser.normalise(raw, "op-india", CsmsEvent.IngestionSource.WEBHOOK);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("FAILURE MODE: missing charger_id → skipped, not crash")
    void normalise_missingChargerId_returnsEmpty() {
        Map<String, Object> raw = new HashMap<>();
        // charger_id absent
        raw.put("connector_id", "C1");
        raw.put("type", "status");
        raw.put("status", "Available");
        raw.put("ts", "2024-01-15T10:30:00Z");

        Optional<CsmsEvent> result = normaliser.normalise(raw, "op-india", CsmsEvent.IngestionSource.WEBHOOK);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("FAILURE MODE: unparseable timestamp → skipped, not crash")
    void normalise_badTimestamp_returnsEmpty() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("charger_id", "CHR-001");
        raw.put("connector_id", "C1");
        raw.put("type", "status");
        raw.put("status", "Available");
        raw.put("ts", "not-a-timestamp");

        Optional<CsmsEvent> result = normaliser.normalise(raw, "op-india", CsmsEvent.IngestionSource.POLL);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("FAILURE MODE: null event map → skipped, not crash")
    void normalise_nullEvent_returnsEmpty() {
        Optional<CsmsEvent> result = normaliser.normalise(null, "op-india", CsmsEvent.IngestionSource.POLL);
        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("FAILURE MODE: empty event map → skipped, not crash")
    void normalise_emptyEvent_returnsEmpty() {
        Optional<CsmsEvent> result = normaliser.normalise(Map.of(), "op-india", CsmsEvent.IngestionSource.POLL);
        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("FAILURE MODE: STATUS event with null status field → skipped")
    void normalise_statusEvent_nullStatus_returnsEmpty() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("charger_id", "CHR-001");
        raw.put("connector_id", "C1");
        raw.put("type", "status");
        raw.put("status", null);  // required for STATUS type
        raw.put("ts", "2024-01-15T10:30:00Z");

        Optional<CsmsEvent> result = normaliser.normalise(raw, "op-india", CsmsEvent.IngestionSource.WEBHOOK);
        assertThat(result).isEmpty();
    }

    // ─── EVENT KEY DETERMINISM ─────────────────────────────────────────────────

    @Test
    @DisplayName("Event key is deterministic — same input produces same key")
    void normalise_sameInput_sameEventKey() {
        Map<String, Object> raw = Map.of(
            "charger_id", "CHR-001",
            "connector_id", "C1",
            "type", "status",
            "status", "Charging",
            "ts", "2024-01-15T10:30:00Z"
        );

        Optional<CsmsEvent> r1 = normaliser.normalise(new HashMap<>(raw), "op-1", CsmsEvent.IngestionSource.WEBHOOK);
        Optional<CsmsEvent> r2 = normaliser.normalise(new HashMap<>(raw), "op-1", CsmsEvent.IngestionSource.POLL);

        assertThat(r1).isPresent();
        assertThat(r2).isPresent();
        // Same semantic event from different sources → same key (idempotency key is content-based)
        assertThat(r1.get().getEventKey()).isEqualTo(r2.get().getEventKey());
    }

    @Test
    @DisplayName("Different events produce different event keys")
    void normalise_differentEvents_differentKeys() {
        Map<String, Object> raw1 = new HashMap<>(Map.of(
            "charger_id", "CHR-001", "connector_id", "C1",
            "type", "status", "status", "Charging",
            "ts", "2024-01-15T10:30:00Z"
        ));
        Map<String, Object> raw2 = new HashMap<>(Map.of(
            "charger_id", "CHR-001", "connector_id", "C1",
            "type", "status", "status", "Available",
            "ts", "2024-01-15T10:35:00Z"
        ));

        Optional<CsmsEvent> r1 = normaliser.normalise(raw1, "op-1", CsmsEvent.IngestionSource.WEBHOOK);
        Optional<CsmsEvent> r2 = normaliser.normalise(raw2, "op-1", CsmsEvent.IngestionSource.WEBHOOK);

        assertThat(r1.get().getEventKey()).isNotEqualTo(r2.get().getEventKey());
    }
}
