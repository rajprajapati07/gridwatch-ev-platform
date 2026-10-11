package com.gridwatch.config;

import com.gridwatch.model.CsmsEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka producer and topic configuration.
 *
 * Topic design: partitioned by charger_id to guarantee per-charger ordering.
 * At 12,000 chargers with 5K events/s, 12 partitions give ~400 events/s/partition —
 * well within a single consumer thread's capacity (~10K events/s per thread).
 *
 * Durability: acks=all + min.insync.replicas=2 + enable.idempotence=true
 * ensures no data loss and exactly-once producer semantics.
 */
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${gridwatch.kafka.topic:csms.events.raw}")
    private String topicName;

    @Bean
    public NewTopic csmsEventsTopic() {
        return TopicBuilder.name(topicName)
            .partitions(12)
            .replicas(3)
            .config("retention.ms", String.valueOf(7 * 24 * 60 * 60 * 1000L)) // 7 days
            .config("min.insync.replicas", "2")
            .build();
    }

    @Bean
    public ProducerFactory<String, CsmsEvent> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, CsmsEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
