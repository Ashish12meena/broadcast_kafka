package com.aigreentick.services.broadcast.client.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Request body sent to messaging service's callback endpoint
 * after processing a window of recipients (up to 80).
 *
 * POST /internal/broadcast/callbacks/message-results
 *
 * Called once per window (every 80 recipients), not once per full Kafka batch.
 * Messaging Service performs a single bulk UPDATE for all results in one call.
 */
@Data
@Builder
public class MessageResultCallbackRequest {

    @JsonProperty("campaign_id")
    private Long campaignId;

    /**
     * The phone number that sent these messages.
     * Identifies which phone number's window these results belong to.
     */
    @JsonProperty("phone_number_id")
    private String phoneNumberId;

    private List<RecipientResult> results;

    @Data
    @Builder
    public static class RecipientResult {

        @JsonProperty("recipient_id")
        private Long recipientId;

        @JsonProperty("message_id")
        private Long messageId;

        // @JsonProperty("contact_id")
        // private Long contactId;

        /**
         * true = Meta accepted the message, false = Meta rejected it.
         */
        private boolean success;

        /**
         * Populated when success=true. wamid from Meta response.
         * Used by Messaging Service for deduplication on Kafka re-delivery.
         */
        @JsonProperty("provider_message_id")
        private String providerMessageId;

        /**
         * Populated when success=false.
         */
        @JsonProperty("error_code")
        private String errorCode;

        @JsonProperty("error_message")
        private String errorMessage;
    }
}