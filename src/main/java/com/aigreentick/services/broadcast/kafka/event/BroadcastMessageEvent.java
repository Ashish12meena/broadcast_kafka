package com.aigreentick.services.broadcast.kafka.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

/**
 * Kafka event received from messaging service on topic: whatsapp.messages.outbound
 *
 * Key:   phone_number_id (partition affinity — all batches for same phone number
 *        go to same partition, preserving order and isolating rate limits)
 *
 * phone_number_id drives everything in the broadcast service:
 *   - Kafka partition key
 *   - Per-phone WabaQueue key
 *   - Semaphore key (Meta enforces 80 concurrent requests per phone number)
 *   - Meta API path param: POST /{phoneNumberId}/messages
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BroadcastMessageEvent {

    @JsonProperty("campaign_id")
    private Long campaignId;

    /**
     * The sending phone number ID registered with Meta.
     * Single identifier used for queuing, rate limiting, and API calls.
     */
    @JsonProperty("phone_number_id")
    private String phoneNumberId;

    /**
     * Bearer token for Meta API authentication.
     * Embedded by Messaging Service at dispatch time — no credential lookup needed here.
     */
    @JsonProperty("access_token")
    private String accessToken;

    /**
     * Recipients in this Kafka batch (up to 1000).
     * Processed as windows of 80 (= Meta's per-phone-number concurrency limit).
     */
    private List<RecipientPayload> payloads;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RecipientPayload {

        @JsonProperty("recipient_id")
        private Long recipientId;

        @JsonProperty("message_id")
        private Long messageId;

        @JsonProperty("contact_id")
        private Long contactId;

        /**
         * Complete Meta API request body (JSON string, snake_case).
         * Pre-built by Messaging Service during campaign preparation.
         * Ready to POST to /{phoneNumberId}/messages as-is.
         */
        @JsonProperty("request_payload")
        private String requestPayload;
    }
}