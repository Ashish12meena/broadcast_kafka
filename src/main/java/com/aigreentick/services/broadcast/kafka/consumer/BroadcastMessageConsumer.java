package com.aigreentick.services.broadcast.kafka.consumer;


import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.aigreentick.services.broadcast.service.BatchCoordinatorService;
import com.aigreentick.services.messaging.broadcast.kafka.event.BroadcastReportEvent;
import com.aigreentick.services.messaging.broadcast.service.impl.BatchCoordinator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka consumer for broadcast messages.
 * 
 * 
 * - Consumer thread just adds event to batch and returns immediately
 * - BatchCoordinator handles all batching, WhatsApp calls, and DB updates
 * - Semaphores managed efficiently (held only during WhatsApp calls)
 * - Database updates done in single transaction per batch
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BroadcastMessageConsumer {

    private final BatchCoordinatorService batchCoordinator;

    @KafkaListener(
        topics = "${kafka.topics.campaign-messages.name}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "campaignKafkaListenerFactory"
    )
    public void consumeCampaignMessage(
            @Payload BroadcastReportEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.info("Received message: broadcastId={} recipient={} partition={} offset={}",
            event.getBroadcastId(), event.getRecipient(), partition, offset);

        try {
            // Simply add to batch - returns immediately
            // BatchCoordinator handles:
            // 1. Batching events (80 max)
            // 2. Acquiring semaphores
            // 3. Sending WhatsApp requests concurrently
            // 4. Releasing semaphores
            // 5. Batch updating database
            // 6. Acknowledging messages
            batchCoordinator.addEventToBatch(event, acknowledgment);

        } catch (Exception e) {
            log.error("Failed to add event to batch. broadcastId={} recipient={} partition={} offset={}",
                event.getBroadcastId(), event.getRecipient(), partition, offset, e);
            
            // Acknowledge to prevent infinite retry
            acknowledgment.acknowledge();
        }
    }
}