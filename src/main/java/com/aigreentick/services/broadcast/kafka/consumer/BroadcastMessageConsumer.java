package com.aigreentick.services.broadcast.kafka.consumer;

import com.aigreentick.services.broadcast.kafka.event.BroadcastMessageEvent;
import com.aigreentick.services.broadcast.service.BatchCoordinatorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for outbound broadcast messages.
 *
 * Topic:   whatsapp.messages.outbound
 * Key:     waba_account_id
 * Value:   JSON { campaign_id, waba_account_id, payloads: [...] }
 *
 * Each Kafka message contains a BATCH of recipients for one campaign + WABA account.
 * The consumer deserializes and hands off to BatchCoordinatorService immediately.
 * All heavy lifting (Meta API calls, callbacks) happens in the coordinator.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BroadcastMessageConsumer {

    private final BatchCoordinatorService batchCoordinator;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${kafka.topics.outbound-messages:whatsapp.messages.outbound}",
            groupId = "${spring.kafka.consumer.group-id:broadcast-service}",
            containerFactory = "broadcastKafkaListenerFactory"
    )
    public void consume(
            @Payload String rawMessage,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String wabaAccountId,
            Acknowledgment acknowledgment) {

        log.info("Received broadcast batch: wabaAccountId={} partition={} offset={}",
                wabaAccountId, partition, offset);

        try {
            BroadcastMessageEvent event = objectMapper.readValue(rawMessage, BroadcastMessageEvent.class);

            log.info("Parsed broadcast event: campaignId={} wabaAccountId={} recipients={}",
                    event.getCampaignId(),
                    event.getWabaAccountId(),
                    event.getPayloads() != null ? event.getPayloads().size() : 0);

            // Hand off to batch coordinator — returns immediately.
            // Coordinator handles: Meta API calls, callbacks to messaging service, Kafka ack.
            batchCoordinator.addBatch(event, acknowledgment);

        } catch (Exception e) {
            log.error("Failed to parse broadcast event: wabaAccountId={} partition={} offset={} error={}",
                    wabaAccountId, partition, offset, e.getMessage(), e);

            // Acknowledge to avoid infinite reprocessing of a malformed message
            acknowledgment.acknowledge();
        }
    }
}