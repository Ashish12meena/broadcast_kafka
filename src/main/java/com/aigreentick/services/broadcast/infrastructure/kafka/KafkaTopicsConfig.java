package com.aigreentick.services.broadcast.infrastructure.kafka;

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
@ConditionalOnProperty(value = "broadcast.kafka.auto-create-topics", havingValue = "true")
public class KafkaTopicsConfig {

    /**
     * Divisible by 2, 3, 4, 6, 8 and 12, so instance counts in that range each get an equal share of
     * partitions rather than an uneven split where one pod does twice the work.
     */
    private static final int DISPATCH_PARTITIONS = 24;

    @Bean
    public NewTopic outboundMessagesTopic(BroadcastProperties properties) {
        return TopicBuilder.name(properties.topics().outboundMessages())
                .partitions(DISPATCH_PARTITIONS)
                .replicas(1)
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
                .partitions(3)
                .replicas(1)
                .configs(Map.of(
                        "cleanup.policy", "compact",
                        "min.cleanable.dirty.ratio", "0.1",
                        "segment.ms", "60000"))
                .build();
    }

    @Bean
    public NewTopic messageResultsTopic(BroadcastProperties properties) {
        return TopicBuilder.name(properties.topics().messageResults())
                .partitions(DISPATCH_PARTITIONS)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic deadLetterTopic(BroadcastProperties properties) {
        return TopicBuilder.name(properties.topics().deadLetter())
                .partitions(3)
                .replicas(1)
                // Long retention: a dead letter is investigated by a human, on human timescales.
                .config("retention.ms", String.valueOf(30L * 24 * 60 * 60 * 1000))
                .build();
    }
}
