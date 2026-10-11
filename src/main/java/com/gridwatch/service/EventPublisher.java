package com.gridwatch.service;

import com.gridwatch.model.CsmsEvent;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Publishes normalised CsmsEvents to the Kafka topic.
 * Partitioned by charger_id to ensure ordering guarantees per charger.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventPublisher {

    private final KafkaTemplate<String, CsmsEvent> kafkaTemplate;
    private final MeterRegistry meterRegistry;

    @Value("${gridwatch.kafka.topic:csms.events.raw}")
    private String topic;

    public void publish(CsmsEvent event) {
        kafkaTemplate.send(topic, event.getChargerId(), event)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish event key={} charger={}: {}",
                        event.getEventKey(), event.getChargerId(), ex.getMessage());
                    meterRegistry.counter("gridwatch.kafka.publish.failed",
                        "event_type", event.getType().name()).increment();
                } else {
                    meterRegistry.counter("gridwatch.kafka.publish.success",
                        "event_type", event.getType().name()).increment();
                }
            });
    }
}
