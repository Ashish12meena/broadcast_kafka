package com.aigreentick.services.broadcast.kafka.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

/**
 * Kafka event received from messaging service on topic: whatsapp.messages.outbound
 * Key: waba_account_id (for rate limiting + partitioning)
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BroadcastMessageEvent {

    @JsonProperty("campaign_id")
    private Long campaignId;

    @JsonProperty("waba_account_id")
    private Long wabaAccountId;

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
         * Ready to POST to /{phoneNumberId}/messages as-is.
         */
        @JsonProperty("request_payload")
        private String requestPayload;
    }
}