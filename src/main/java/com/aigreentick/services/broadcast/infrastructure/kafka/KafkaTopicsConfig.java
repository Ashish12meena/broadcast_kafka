package com.aigreentick.services.broadcast.infrastructure.kafka;

import com.aigreentick.services.broadcast.common.constants.InfraConstants;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;

/**
 * Topic definitions, created at startup in environments that allow it.
 *
 * <p>Disabled by default. In production the topics are provisioned deliberately, with partition
 * counts chosen for the expected throughput — a service creating its own topics with default
 * settings is how a cluster ends up with a one-partition topic that cannot be widened without
 * downtime. Enable it for local development, where the convenience is worth more.
 */
@Configuration
@ConditionalOnProperty(
        value = InfraConstants.ConfigKeys.KAFKA_AUTO_CREATE_TOPICS,
        havingValue = InfraConstants.ConfigKeys.ENABLED_TRUE)
public class KafkaTopicsConfig {

    @Bean
    public NewTopic outboundMessagesTopic(BroadcastProperties properties) {
        return TopicBuilder.name(properties.topics().outboundMessages())
                .partitions(InfraConstants.Kafka.DISPATCH_PARTITIONS)
                .replicas(InfraConstants.Kafka.DEFAULT_REPLICAS)
                .build();
    }

    /**
     * Compacted, so the topic holds the current capacity of every phone number indefinitely rather
     * than a window of recent changes. That is what lets a restarting instance rebuild its whole
     * picture from the log with no API call to anyone.
     */
    @Bean
    public NewTopic capacityUpdatesTopic(BroadcastProperties properties) {
        return TopicBuilder.name(properties.topics().capacityUpdates())
                .partitions(InfraConstants.Kafka.LOW_VOLUME_PARTITIONS)
                .replicas(InfraConstants.Kafka.DEFAULT_REPLICAS)
                .configs(Map.of(
                        InfraConstants.Kafka.CONFIG_CLEANUP_POLICY, InfraConstants.Kafka.CLEANUP_POLICY_COMPACT,
                        InfraConstants.Kafka.CONFIG_MIN_CLEANABLE_DIRTY_RATIO,
                        InfraConstants.Kafka.MIN_CLEANABLE_DIRTY_RATIO,
                        InfraConstants.Kafka.CONFIG_SEGMENT_MS, InfraConstants.Kafka.CAPACITY_SEGMENT_MS))
                .build();
    }

    @Bean
    public NewTopic messageResultsTopic(BroadcastProperties properties) {
        return TopicBuilder.name(properties.topics().messageResults())
                .partitions(InfraConstants.Kafka.DISPATCH_PARTITIONS)
                .replicas(InfraConstants.Kafka.DEFAULT_REPLICAS)
                .build();
    }

    @Bean
    public NewTopic deadLetterTopic(BroadcastProperties properties) {
        return TopicBuilder.name(properties.topics().deadLetter())
                .partitions(InfraConstants.Kafka.LOW_VOLUME_PARTITIONS)
                .replicas(InfraConstants.Kafka.DEFAULT_REPLICAS)
                // Long retention: a dead letter is investigated by a human, on human timescales.
                .config(InfraConstants.Kafka.CONFIG_RETENTION_MS,
                        String.valueOf(InfraConstants.Kafka.DEAD_LETTER_RETENTION_MS))
                .build();
    }
}
