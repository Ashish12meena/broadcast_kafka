package com.aigreentick.services.broadcast.infrastructure.kafka.listener;

import com.aigreentick.services.broadcast.application.port.in.DispatchBatchUseCase;
import com.aigreentick.services.broadcast.application.port.out.DeadLetterPort;
import com.aigreentick.services.broadcast.application.service.ingest.ConsumerFlowController;
import com.aigreentick.services.broadcast.domain.model.DispatchBatch;
import com.aigreentick.services.broadcast.domain.model.Recipient;
import com.aigreentick.services.broadcast.infrastructure.kafka.event.DispatchEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Reads batches from the outbound topic.
 *
 * <p>Deserialises and hands off, nothing more. Anything slow on this thread stops the poll loop for
 * every partition assigned to it, and a poll loop that stalls past {@code max.poll.interval.ms}
 * triggers a rebalance and the redelivery of work that was being processed perfectly well.
 *
 * <p>The offset is not acknowledged here. It moves when the last recipient in the batch has an
 * outcome, which may be many seconds later on another thread. Kafka has no partial acknowledgement,
 * so committing early would discard the recipients that had not been sent yet.
 */
@Component
public class DispatchEventListener {

    private static final Logger log = LoggerFactory.getLogger(DispatchEventListener.class);

    private final DispatchBatchUseCase dispatchBatch;
    private final DeadLetterPort deadLetter;
    private final ObjectMapper objectMapper;

    public DispatchEventListener(
            DispatchBatchUseCase dispatchBatch, DeadLetterPort deadLetter, ObjectMapper objectMapper) {
        this.dispatchBatch = dispatchBatch;
        this.deadLetter = deadLetter;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            id = ConsumerFlowController.DISPATCH_LISTENER_ID,
            topics = "${broadcast.topics.outbound-messages}",
            containerFactory = "dispatchListenerFactory")
    public void onDispatchEvent(
            @Payload String rawMessage,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        MDC.put("kafkaKey", String.valueOf(key));
        MDC.put("partition", String.valueOf(partition));
        MDC.put("offset", String.valueOf(offset));

        try {
            DispatchEvent event = objectMapper.readValue(rawMessage, DispatchEvent.class);

            String validationError = event.validationError();
            if (validationError != null) {
                // Set aside rather than dropped. Silently discarding loses up to a full batch of
                // recipients with no record they ever existed, and retrying forever blocks the
                // partition behind a message that can never succeed.
                sendToDeadLetter(rawMessage, validationError, partition, offset, acknowledgment);
                return;
            }

            dispatchBatch.accept(toDomain(event), acknowledgment::acknowledge);

        } catch (Exception e) {
            sendToDeadLetter(rawMessage, "deserialization failed: " + e.getMessage(),
                    partition, offset, acknowledgment);
        } finally {
            MDC.clear();
        }
    }

    private DispatchBatch toDomain(DispatchEvent event) {
        List<Recipient> recipients = event.payloads().stream()
                .map(item -> new Recipient(
                        item.recipientId(), item.messageId(), item.contactId(), item.requestPayload()))
                .toList();

        return new DispatchBatch(
                event.campaignId(),
                event.phoneNumberId(),
                event.wabaAccountId(),
                event.accessToken(),
                recipients);
    }

    private void sendToDeadLetter(
            String rawMessage, String reason, int partition, long offset, Acknowledgment acknowledgment) {

        log.error("Dispatch event rejected, sending to dead letter topic: {}", reason);
        try {
            deadLetter.send(rawMessage, reason, "outbound-messages", partition, offset);
        } finally {
            // Acknowledged only after the dead letter is safely away, so a broker failure leaves the
            // message on the source topic rather than losing it from both.
            acknowledgment.acknowledge();
        }
    }
}
