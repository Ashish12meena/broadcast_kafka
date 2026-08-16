package com.aigreentick.services.broadcast.infrastructure.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumer wiring for the two inbound topics.
 *
 * <p>Two separate factories because the topics have opposite requirements. Dispatch needs manual
 * acknowledgement, since an offset may only move once every recipient in the batch is resolved.
 * Capacity is a compacted stream of current values where automatic acknowledgement is right and
 * replaying from the beginning on every start is exactly what is wanted.
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:broadcast-service}")
    private String groupId;

    @Value("${spring.kafka.consumer.max-poll-records:100}")
    private int maxPollRecords;

    @Value("${broadcast.kafka.dispatch-concurrency:6}")
    private int dispatchConcurrency;

    @Bean
    public ConsumerFactory<String, String> dispatchConsumerFactory() {
        Map<String, Object> config = baseConsumerConfig(groupId);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean(name = "dispatchListenerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> dispatchListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(dispatchConsumerFactory());

        // The offset moves only when the batch is fully resolved, which happens on another thread
        // long after the listener method returns.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Should match the partition count of the outbound topic.
        factory.setConcurrency(dispatchConcurrency);
        return factory;
    }

    /**
     * Capacity consumer.
     *
     * <p>A unique group per instance, on purpose. This is not work to be divided up — every instance
     * needs every capacity update, so each one reads the whole compacted topic independently.
     */
    @Bean
    public ConsumerFactory<String, String> capacityConsumerFactory() {
        Map<String, Object> config = baseConsumerConfig(
                groupId + "-capacity-" + java.util.UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean(name = "capacityListenerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> capacityListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(capacityConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.setConcurrency(1);
        return factory;
    }

    private Map<String, Object> baseConsumerConfig(String consumerGroup) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        return config;
    }
}
