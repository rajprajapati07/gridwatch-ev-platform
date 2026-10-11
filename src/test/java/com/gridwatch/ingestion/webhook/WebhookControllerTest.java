package com.gridwatch.ingestion.webhook;

import com.gridwatch.ingestion.poll.CsmsEventNormaliser;
import com.gridwatch.model.CsmsEvent;
import com.gridwatch.service.EventPublisher;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tests webhook HMAC verification and batch processing.
 */
@ExtendWith(MockitoExtension.class)
class WebhookControllerTest {

    @Mock CsmsEventNormaliser normaliser;
    @Mock EventPublisher eventPublisher;

    private WebhookController controller;

    private static final String SECRET = "test-webhook-secret";

    @BeforeEach
    void setUp() throws Exception {
        var registry = new SimpleMeterRegistry();
        controller = new WebhookController(
            new com.fasterxml.jackson.databind.ObjectMapper(),
            normaliser,
            eventPublisher,
            registry
        );
        // Inject secret via reflection (Spring @Value not active in unit test)
        var field = WebhookController.class.getDeclaredField("webhookSecret");
        field.setAccessible(true);
        field.set(controller, SECRET);
    }

    @Test
    @DisplayName("Valid HMAC signature → 200 and events published")
    void receive_validSignature_returns200() throws Exception {
        String body = "{\"delivery_id\":\"d1\",\"events\":[{\"charger_id\":\"CHR-001\",\"connector_id\":\"C1\",\"type\":\"status\",\"status\":\"Available\",\"ts\":\"2024-01-15T10:00:00Z\"}]}";
        String sig = hmacSha256(SECRET, body);

        CsmsEvent mockEvent = new CsmsEvent();
        mockEvent.setEventKey("key1");
        mockEvent.setChargerId("CHR-001");
        when(normaliser.normalise(any(), eq("op-india"), eq(CsmsEvent.IngestionSource.WEBHOOK)))
            .thenReturn(Optional.of(mockEvent));

        ResponseEntity<Void> response = controller.receive(
            "op-india", sig, body.getBytes(StandardCharsets.UTF_8), null);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(eventPublisher).publish(mockEvent);
    }

    @Test
    @DisplayName("Invalid HMAC signature → 401, no events published")
    void receive_invalidSignature_returns401() throws Exception {
        String body = "{\"delivery_id\":\"d1\",\"events\":[]}";
        String wrongSig = "deadbeef0000000000000000000000000000000000000000000000000000dead";

        ResponseEntity<Void> response = controller.receive(
            "op-india", wrongSig, body.getBytes(StandardCharsets.UTF_8), null);

        assertThat(response.getStatusCode().value()).isEqualTo(401);
        verifyNoInteractions(eventPublisher);
        verifyNoInteractions(normaliser);
    }

    @Test
    @DisplayName("Malformed events in batch → other events still published (batch not rejected)")
    void receive_malformedEventInBatch_otherEventsPublished() throws Exception {
        // Batch with one good event and one malformed (normaliser returns empty for malformed)
        String body = """
            {
              "delivery_id": "d2",
              "events": [
                {"charger_id":"CHR-001","connector_id":"C1","type":"status","status":"Charging","ts":"2024-01-15T10:00:00Z"},
                {"charger_id":"CHR-002","connector_id":null,"type":"meter_value","energy_register_wh":null,"ts":"2024-01-15T10:00:01Z"}
              ]
            }
            """;
        String sig = hmacSha256(SECRET, body);

        CsmsEvent goodEvent = new CsmsEvent();
        goodEvent.setEventKey("good-key");
        goodEvent.setChargerId("CHR-001");

        when(normaliser.normalise(any(), any(), any()))
            .thenReturn(Optional.of(goodEvent))   // first event normalises fine
            .thenReturn(Optional.empty());          // second event is malformed → skipped

        ResponseEntity<Void> response = controller.receive(
            "op-india", sig, body.getBytes(StandardCharsets.UTF_8), null);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        // Only the good event was published; the malformed one was silently skipped
        verify(eventPublisher, times(1)).publish(goodEvent);
    }

    @Test
    @DisplayName("Malformed JSON body → 200 (don't let CSMS redeliver forever)")
    void receive_malformedJsonBody_returns200AnyWay() throws Exception {
        String body = "this-is-not-json";
        String sig = hmacSha256(SECRET, body);

        ResponseEntity<Void> response = controller.receive(
            "op-india", sig, body.getBytes(StandardCharsets.UTF_8), null);

        // Must return 200 — a 4xx/5xx causes the CSMS to retry indefinitely
        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verifyNoInteractions(eventPublisher);
    }

    private String hmacSha256(String secret, String data) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] bytes = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
