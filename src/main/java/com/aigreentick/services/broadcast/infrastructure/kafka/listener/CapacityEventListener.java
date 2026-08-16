package com.aigreentick.services.broadcast.infrastructure.kafka.listener;

import com.aigreentick.services.broadcast.application.port.in.UpdateCapacityUseCase;
import com.aigreentick.services.broadcast.domain.model.CapacitySource;
import com.aigreentick.services.broadcast.domain.model.PhoneNumberCapacity;
import com.aigreentick.services.broadcast.infrastructure.kafka.event.CapacityEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Applies capacity updates published by the Messaging Service.
 *
 * <p>A malformed capacity event is logged and skipped rather than dead-lettered. The consequence of
 * skipping is that a number keeps its previous rate, which is the value that was correct a moment
 * ago — a good deal safer than stopping the stream and leaving every subsequent update unapplied.
 */
@Component
public class CapacityEventListener {

    private static final Logger log = LoggerFactory.getLogger(CapacityEventListener.class);

    private final UpdateCapacityUseCase updateCapacity;
    private final ObjectMapper objectMapper;

    public CapacityEventListener(UpdateCapacityUseCase updateCapacity, ObjectMapper objectMapper) {
        this.updateCapacity = updateCapacity;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            id = "broadcast-capacity-listener",
            topics = "${broadcast.topics.capacity-updates}",
            containerFactory = "capacityListenerFactory")
    public void onCapacityEvent(@Payload String rawMessage) {
        try {
            // Compaction tombstones arrive as null values and mean the number was removed. Nothing
            // to apply: the existing entry expires on its own TTL.
            if (rawMessage == null || rawMessage.isBlank()) {
                return;
            }

            CapacityEvent event = objectMapper.readValue(rawMessage, CapacityEvent.class);
            if (!event.isUsable()) {
                log.warn("Ignoring capacity event with no usable rate: {}", rawMessage);
                return;
            }

            updateCapacity.apply(toDomain(event));

        } catch (Exception e) {
            log.error("Could not apply capacity event; the previous rate stays in effect", e);
        }
    }

    private PhoneNumberCapacity toDomain(CapacityEvent event) {
        int effective = event.effectiveMps();
        int configured = event.configuredMps() == null ? effective : event.configuredMps();

        return new PhoneNumberCapacity(
                event.phoneNumberId(),
                configured,
                // The working rate can never exceed the ceiling, whatever the producer said.
                Math.min(effective, configured),
                event.tier(),
                event.backoffUntilMs() == null ? 0L : event.backoffUntilMs(),
                event.updatedAtMs() == null ? System.currentTimeMillis() : event.updatedAtMs(),
                CapacitySource.CAPACITY_EVENT);
    }
}
