package com.aigreentick.services.broadcast.infrastructure.kafka.publisher;

import com.aigreentick.services.broadcast.common.constants.DomainConstants;
import com.aigreentick.services.broadcast.application.port.out.ResultPublisherPort;
import com.aigreentick.services.broadcast.domain.model.BatchResult;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import com.aigreentick.services.broadcast.infrastructure.kafka.event.ResultEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Publishes send outcomes back to the Messaging Service.
 *
 * <h2>Kafka rather than an HTTP callback</h2>
 * These outcomes are the record that a customer was messaged. Losing one leaves a row stuck
 * mid-flight, and the recovery for that is to send the same customer the same thing again. An HTTP
 * post would need bounded retry plus a durable local buffer to be safe — a less capable Kafka,
 * built by hand. The broker is already a dependency.
 *
 * <h2>Asynchronous now, and the batching moved to the producer</h2>
 * This used to block on {@code .get(timeout)}, which was correct given a caller that buffered in
 * heap and flushed a few hundred at a time. The buffer is gone, so blocking here would mean a send
 * thread waiting on a broker round trip per recipient.
 *
 * <p>Returning the future instead lets {@code ResultCollector} publish immediately and join every
 * outstanding future once, before the Kafka offset moves. The wire efficiency the buffer provided
 * now comes from {@code linger.ms} and {@code batch.size} on the producer — which is where record
 * batching belongs, and which is durable in a way a heap list never was.
 *
 * <p>Keyed on campaign so a campaign's outcomes arrive in order and land on one partition.
 */
@Component
public class ResultEventPublisher implements ResultPublisherPort {

    private static final Logger log = LoggerFactory.getLogger(ResultEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final BroadcastProperties properties;

    public ResultEventPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            BroadcastProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    @Override
    public CompletableFuture<Void> publishAsync(BatchResult result) {
        ResultEvent event = new ResultEvent(
                result.campaignId(),
                result.phoneNumberId(),
                result.traceId(),
                result.outcomes().stream()
                        .map(outcome -> new ResultEvent.ResultItem(
                                outcome.recipientId(),
                                outcome.messageId(),
                                outcome.contactId(),
                                outcome.success(),
                                outcome.providerMessageId(),
                                outcome.messageStatus(),
                                outcome.errorCode(),
                                outcome.errorMessage(),
                                outcome.retryable(),
                                outcome.attempts(),
                                outcome.sentAtMs()))
                        .toList());

        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            // Serialisation cannot succeed on a retry, so this fails immediately rather than being
            // handed to the broker. A failed future is still the right shape: the caller must not
            // acknowledge the offset either way.
            return CompletableFuture.failedFuture(new IllegalStateException(
                    DomainConstants.Messages.RESULT_PUBLISH_FAILED_FORMAT
                            .formatted(result.campaignId()), e));
        }

        return kafkaTemplate
                .send(properties.topics().messageResults(),
                        String.valueOf(result.campaignId()), payload)
                .thenAccept(sent -> log.debug(
                        "Published {} result(s) campaignId={} phoneNumberId={} traceId={}",
                        result.outcomes().size(), result.campaignId(),
                        result.phoneNumberId(), result.traceId()))
                .exceptionallyCompose(error -> CompletableFuture.failedFuture(
                        new IllegalStateException(
                                DomainConstants.Messages.RESULT_PUBLISH_FAILED_FORMAT
                                        .formatted(result.campaignId()), error)));
    }
}
