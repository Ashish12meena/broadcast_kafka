package com.aigreentick.services.broadcast.infrastructure.kafka.publisher;

import com.aigreentick.services.broadcast.application.port.out.ResultPublisherPort;
import com.aigreentick.services.broadcast.domain.model.BatchResult;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import com.aigreentick.services.broadcast.infrastructure.kafka.event.ResultEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Publishes send outcomes back to the Messaging Service.
 *
 * <h2>Kafka rather than an HTTP callback</h2>
 * These outcomes are the record that a customer was messaged. Losing one leaves a row stuck
 * mid-flight, and the recovery for that is to send the same customer the same thing again. An HTTP
 * post would need bounded retry plus a durable local buffer to be safe — which is a less capable
 * Kafka, built by hand. The broker is already a dependency here.
 *
 * <h2>Waiting for the broker</h2>
 * The send is confirmed synchronously. The caller is about to acknowledge a Kafka offset on the
 * strength of these results having been published; doing that on the strength of a callback that has
 * merely been handed to a buffer would be the same silent-loss problem in a different place.
 *
 * <p>Keyed on campaign so that a campaign's outcomes arrive in order and land on one partition.
 */
@Component
public class ResultEventPublisher implements ResultPublisherPort {

    private static final Logger log = LoggerFactory.getLogger(ResultEventPublisher.class);

    private static final long PUBLISH_TIMEOUT_SECONDS = 30;

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
    public void publish(BatchResult result) {
        ResultEvent event = new ResultEvent(
                result.campaignId(),
                result.phoneNumberId(),
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

        try {
            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate
                    .send(properties.topics().messageResults(), String.valueOf(result.campaignId()), payload)
                    .get(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            log.debug("Published {} results campaignId={} phoneNumberId={}",
                    result.outcomes().size(), result.campaignId(), result.phoneNumberId());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing results", e);
        } catch (Exception e) {
            // Thrown on purpose. The caller must not acknowledge a batch whose outcomes were never
            // recorded anywhere.
            throw new IllegalStateException(
                    "Could not publish results for campaign " + result.campaignId(), e);
        }
    }
}
