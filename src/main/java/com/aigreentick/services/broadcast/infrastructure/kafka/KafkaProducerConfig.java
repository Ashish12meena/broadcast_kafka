package com.aigreentick.services.broadcast.infrastructure.kafka;

import com.aigreentick.services.broadcast.common.constants.InfraConstants;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Producer wiring for outbound results and dead letters.
 *
 * <p>{@code acks=all} with idempotence enabled, because these are the outcomes of messages that have
 * already reached customers. A result acknowledged by one broker and then lost to a leader election
 * leaves a message row stuck mid-flight, and the recovery for that is to send the customer a second
 * copy. The throughput cost of full acknowledgement is irrelevant at this volume — results are
 * published in groups, not per message.
 */
@Configuration
public class KafkaProducerConfig {

    @Value(InfraConstants.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS)
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, InfraConstants.Kafka.ACKS_ALL);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, InfraConstants.Kafka.DELIVERY_TIMEOUT_MS);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                InfraConstants.Kafka.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, InfraConstants.Kafka.COMPRESSION_SNAPPY);
        config.put(ProducerConfig.LINGER_MS_CONFIG, InfraConstants.Kafka.LINGER_MS);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
