package com.aigreentick.services.broadcast.infrastructure.kafka.listener;

import com.aigreentick.services.broadcast.common.constants.DomainConstants;
import com.aigreentick.services.broadcast.common.constants.InfraConstants;
import com.aigreentick.services.broadcast.common.constants.ObservabilityConstants;
import com.aigreentick.services.broadcast.application.port.in.DispatchBatchUseCase;
import com.aigreentick.services.broadcast.application.port.out.DeadLetterPort;
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
import java.util.Map;

/**
 * Reads batches from the outbound topic.
 *
 * <p>Deserialises and hands off, nothing more. Anything slow on this thread stops the poll loop for
 * every partition assigned to it, and a poll loop that stalls past {@code max.poll.interval.ms}
 * triggers a rebalance and the redelivery of work that was being processed perfectly well.
 *
 * <p>The offset is not acknowledged here. It moves when the last recipient in the batch has a
 * <em>durable</em> outcome, which may be many seconds later on another thread. Kafka has no partial
 * acknowledgement, so committing early would discard the recipients that had not been sent yet.
 *
 * <h2>The trace id goes into MDC on arrival</h2>
 * This is the point at which a campaign run crosses a service boundary, and it was previously the
 * one link in the chain with nothing to correlate on — both services logged a campaign id and
 * neither logged anything tying one run together. Putting it on the MDC here means every log line
 * this batch produces downstream, on any thread, carries it.
 */
@Component
public class DispatchEventListener {

    private static final Logger log = LoggerFactory.getLogger(DispatchEventListener.class);

    private final DispatchBatchUseCase dispatchBatch;
    private final DeadLetterPort deadLetter;
    private final ObjectMapper objectMapper;

    public DispatchEventListener(DispatchBatchUseCase dispatchBatch, DeadLetterPort deadLetter,
                                 ObjectMapper objectMapper) {
        this.dispatchBatch = dispatchBatch;
        this.deadLetter = deadLetter;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            id = InfraConstants.Kafka.DISPATCH_LISTENER_ID,
            topics = InfraConstants.ConfigKeys.TOPIC_OUTBOUND_MESSAGES,
            containerFactory = InfraConstants.Kafka.DISPATCH_LISTENER_FACTORY)
    public void onDispatchEvent(
            @Payload String rawMessage,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        // Captured rather than cleared on the way out. clear() wipes context belonging to whatever
        // called in, which is harmless while nothing upstream sets MDC and a difficult bug the day
        // a filter or interceptor does.
        Map<String, String> priorContext = MDC.getCopyOfContextMap();

        MDC.put(ObservabilityConstants.Logging.MDC_KAFKA_KEY, String.valueOf(key));
        MDC.put(ObservabilityConstants.Logging.MDC_PARTITION, String.valueOf(partition));
        MDC.put(ObservabilityConstants.Logging.MDC_OFFSET, String.valueOf(offset));

        try {
            DispatchEvent event = objectMapper.readValue(rawMessage, DispatchEvent.class);

            if (event.traceId() != null) {
                MDC.put(ObservabilityConstants.Logging.MDC_TRACE_ID, event.traceId());
            }

            String validationError = event.validationError();
            if (validationError != null) {
                // Set aside rather than dropped. Silently discarding loses up to a full batch of
                // recipients with no record they existed, and retrying forever blocks the partition
                // behind a message that can never succeed.
                sendToDeadLetter(rawMessage, validationError, partition, offset, acknowledgment);
                return;
            }

            dispatchBatch.accept(toDomain(event), acknowledgment::acknowledge);

        } catch (Exception e) {
            sendToDeadLetter(rawMessage,
                    DomainConstants.Messages.DESERIALIZATION_FAILED_PREFIX + e.getMessage(),
                    partition, offset, acknowledgment);
        } finally {
            if (priorContext == null) {
                MDC.clear();
            } else {
                MDC.setContextMap(priorContext);
            }
        }
    }

    private DispatchBatch toDomain(DispatchEvent event) {
        List<Recipient> recipients = event.payloads().stream()
                .map(item -> new Recipient(
                        item.recipientId(), item.messageId(), item.contactId(),
                        item.requestPayload()))
                .toList();

        return new DispatchBatch(
                event.campaignId(),
                event.phoneNumberId(),
                event.wabaAccountId(),
                event.accessToken(),
                event.traceId(),
                recipients);
    }

    private void sendToDeadLetter(String rawMessage, String reason, int partition, long offset,
                                  Acknowledgment acknowledgment) {
        // Reason only. The raw message is a recipient list and belongs on the dead letter topic,
        // not in the log aggregator.
        log.error("Dispatch event rejected, sending to dead letter topic: {}", reason);
        try {
            deadLetter.send(rawMessage, reason,
                    InfraConstants.Kafka.SOURCE_TOPIC_OUTBOUND_MESSAGES, partition, offset);
        } finally {
            // Acknowledged only after the dead letter is safely away, so a broker failure leaves
            // the message on the source topic rather than losing it from both.
            acknowledgment.acknowledge();
        }
    }
}
