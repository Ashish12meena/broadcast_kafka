package com.aigreentick.services.broadcast.infrastructure.kafka.publisher;

import com.aigreentick.services.broadcast.application.port.out.DeadLetterPort;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import com.aigreentick.services.broadcast.infrastructure.observability.BroadcastMetrics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Sets aside messages that cannot be processed.
 *
 * <p>The alternative — acknowledging and moving on — loses up to a full batch of recipients with no
 * record that they existed, which is discovered later only as a campaign whose numbers do not add
 * up. Keeping the original bytes and the reason on a dead letter topic makes the problem
 * inspectable and the messages replayable once the cause is fixed.
 */
@Component
public class DeadLetterEventPublisher implements DeadLetterPort {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterEventPublisher.class);

    private static final long PUBLISH_TIMEOUT_SECONDS = 10;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BroadcastProperties properties;
    private final BroadcastMetrics metrics;

    public DeadLetterEventPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            BroadcastProperties properties,
            BroadcastMetrics metrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.metrics = metrics;
    }

    @Override
    public void send(String rawPayload, String reason, String sourceTopic, int partition, long offset) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                properties.topics().deadLetter(), null, rawPayload);

        record.headers()
                .add(header("dlq-reason", reason))
                .add(header("dlq-source-topic", sourceTopic))
                .add(header("dlq-partition", String.valueOf(partition)))
                .add(header("dlq-offset", String.valueOf(offset)))
                .add(header("dlq-timestamp", String.valueOf(System.currentTimeMillis())));

        try {
            kafkaTemplate.send(record).get(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            metrics.deadLettered(shortReason(reason));
            log.warn("Dead-lettered a message from {}-{} offset {}: {}",
                    sourceTopic, partition, offset, reason);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while dead-lettering a message", e);
        } catch (Exception e) {
            // Nowhere left to put it. Logging the payload is the last resort that keeps it
            // recoverable from the log aggregator.
            log.error("Could not dead-letter a message from {}-{} offset {}. Payload: {}",
                    sourceTopic, partition, offset, rawPayload, e);
        }
    }

    private static RecordHeader header(String key, String value) {
        return new RecordHeader(key, value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8));
    }

    /** Keeps the metric tag to a bounded set of values rather than one per exception message. */
    private static String shortReason(String reason) {
        if (reason == null) {
            return "unknown";
        }
        return reason.startsWith("deserialization") ? "deserialization" : "validation";
    }
}
