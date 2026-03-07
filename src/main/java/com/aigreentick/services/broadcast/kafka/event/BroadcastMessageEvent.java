package com.aigreentick.services.broadcast.kafka.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

/**
 * Kafka event received from messaging service on topic: whatsapp.messages.outbound
 *
 * Messaging service publishes camelCase JSON:
 *   { "campaignId": 42, "wabaAccountId": 123, "accessToken": "...", "payloads": [...] }
 *
 * wabaAccountId from messaging service = phoneNumberId in broadcast service context.
 * This is the same identifier — messaging service calls it wabaAccountId internally,
 * but it's used as phoneNumberId for Meta API calls.
 *
 * phoneNumberId drives everything in the broadcast service:
 *   - Kafka partition key
 *   - Per-phone PhoneQueue key
 *   - Semaphore key (Meta enforces 80 concurrent requests per phone number)
 *   - Meta API path param: POST /{phoneNumberId}/messages
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BroadcastMessageEvent {

    /** Campaign ID from messaging service */
    private Long campaignId;

    /**
     * The sending phone number ID registered with Meta.
     * Messaging service sends this as "wabaAccountId" — mapped here via @JsonProperty.
     * Single identifier used for queuing, rate limiting, and API calls.
     */
    @JsonProperty("wabaAccountId")
    private String phoneNumberId;

    /**
     * Bearer token for Meta API authentication.
     * Embedded by Messaging Service at dispatch time — no credential lookup needed here.
     */
    private String accessToken;

    /**
     * Recipients in this Kafka batch (up to 1000).
     * Processed as windows of 80 (= Meta's per-phone-number concurrency limit).
     */
    private List<RecipientPayload> payloads;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RecipientPayload {

        private Long recipientId;

        private Long messageId;

        private Long contactId;

        /**
         * Complete Meta API request body (JSON string, snake_case).
         * Pre-built by Messaging Service during campaign preparation.
         * Ready to POST to /{phoneNumberId}/messages as-is.
         */
        private String requestPayload;
    }
}