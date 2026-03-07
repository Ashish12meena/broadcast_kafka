package com.aigreentick.services.broadcast.client.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Request body sent to messaging service's callback endpoint
 * after processing a window of recipients (up to 80).
 *
 * POST /internal/broadcast/callbacks/message-results
 *
 * Uses camelCase — inter-service convention.
 * Called once per window (every 80 recipients), not once per full Kafka batch.
 * Messaging Service performs a single bulk UPDATE for all results in one call.
 */
@Data
@Builder
public class MessageResultCallbackRequest {

    private Long campaignId;

    /**
     * The phone number that sent these messages.
     * Identifies which phone number's window these results belong to.
     */
    private String phoneNumberId;

    private List<RecipientResult> results;

    @Data
    @Builder
    public static class RecipientResult {

        private Long recipientId;

        private Long messageId;

        private Long contactId;

        /**
         * true = Meta accepted the message, false = Meta rejected it.
         */
        private boolean success;

        /**
         * wamid from Meta response. Populated when success=true.
         * Used by Messaging Service for deduplication on Kafka re-delivery.
         */
        private String providerMessageId;

        /**
         * Meta's message_status string: "accepted", "sent", etc.
         * Populated when success=true.
         */
        private String messageStatus;

        /**
         * JSON string of the payload sent to Meta.
         * Kept for future auditing — currently not persisted by messaging service.
         */
        private String payload;

        /**
         * JSON string of Meta's raw response.
         * Kept for future auditing — currently not persisted by messaging service.
         */
        private String response;

        /**
         * Populated when success=false.
         */
        private String errorCode;

        private String errorMessage;
    }
}