package com.aigreentick.services.broadcast.client.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Request body sent to messaging service's callback endpoint
 * after processing a batch of recipients.
 *
 * POST /internal/broadcast/callbacks/message-results
 */
@Data
@Builder
public class MessageResultCallbackRequest {

    @JsonProperty("campaign_id")
    private Long campaignId;

    @JsonProperty("waba_account_id")
    private Long wabaAccountId;

    private List<RecipientResult> results;

    @Data
    @Builder
    public static class RecipientResult {

        @JsonProperty("recipient_id")
        private Long recipientId;

        @JsonProperty("message_id")
        private Long messageId;

        @JsonProperty("contact_id")
        private Long contactId;

        /**
         * true = Meta accepted the message, false = Meta rejected it.
         */
        private boolean success;

        /**
         * Populated when success=true. wamid.xxx from Meta response.
         */
        @JsonProperty("provider_message_id")
        private String providerMessageId;

        /**
         * Populated when success=false. Error message from Meta or internal.
         */
        @JsonProperty("error_code")
        private String errorCode;

        @JsonProperty("error_message")
        private String errorMessage;
    }
}