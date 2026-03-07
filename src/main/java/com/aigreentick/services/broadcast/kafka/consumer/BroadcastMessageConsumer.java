package com.aigreentick.services.broadcast.kafka.consumer;

import com.aigreentick.services.broadcast.kafka.event.BroadcastMessageEvent;
import com.aigreentick.services.broadcast.service.BatchCoordinatorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

import org.springframework.context.annotation.Profile;
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
 * Key:     wabaAccountId (String) — from messaging service, used as phoneNumberId here
 * Value:   JSON { campaignId, wabaAccountId, accessToken, payloads: [...] }
 *
 * Each Kafka message contains a batch of up to 1000 recipients for one campaign
 * and one phone number. The consumer deserializes and hands off to
 * BatchCoordinatorService immediately — never blocks the consumer thread.
 */
@Slf4j
@Component
@Profile("kafka")
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
            @Header(KafkaHeaders.RECEIVED_KEY) String kafkaKey,
            Acknowledgment acknowledgment) {

        log.info("Received broadcast batch: kafkaKey={} partition={} offset={}",
                kafkaKey, partition, offset);

        try {
            BroadcastMessageEvent event = objectMapper.readValue(rawMessage, BroadcastMessageEvent.class);

            log.info("Parsed broadcast event: campaignId={} phoneNumberId={} recipients={}",
                    event.getCampaignId(),
                    event.getPhoneNumberId(),
                    event.getPayloads() != null ? event.getPayloads().size() : 0);

            batchCoordinator.addBatch(event, acknowledgment);

        } catch (Exception e) {
            log.error("Failed to parse broadcast event: kafkaKey={} partition={} offset={} error={}",
                    kafkaKey, partition, offset, e.getMessage(), e);

            // Acknowledge to avoid infinite reprocessing of a malformed message
            acknowledgment.acknowledge();
        }
    }
}