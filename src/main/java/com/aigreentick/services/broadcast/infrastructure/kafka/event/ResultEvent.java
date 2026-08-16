package com.aigreentick.services.broadcast.infrastructure.kafka.event;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * What this service publishes back to the Messaging Service.
 *
 * <p>Field names match the Messaging Service's existing callback contract so the receiving logic is
 * unchanged — only the transport differs. Three fields are added: {@code attempts}, {@code retryable}
 * and {@code sentAtMs}.
 *
 * <p>{@code retryable} matters most. This service has already classified the Meta error code to
 * decide whether to retry, and re-deriving that decision on the receiving side would mean two copies
 * of the same catalog drifting apart. {@code errorCode} carries the rate-limit codes that let the
 * Messaging Service degrade the number durably, which is the feedback path that closes the control
 * loop.
 */
public record ResultEvent(
        @JsonProperty("campaignId") Long campaignId,
        @JsonProperty("phoneNumberId") String phoneNumberId,
        @JsonProperty("results") List<ResultItem> results) {

    public record ResultItem(
            @JsonProperty("recipientId") Long recipientId,
            @JsonProperty("messageId") Long messageId,
            @JsonProperty("contactId") Long contactId,
            @JsonProperty("success") boolean success,
            @JsonProperty("providerMessageId") String providerMessageId,
            @JsonProperty("messageStatus") String messageStatus,
            @JsonProperty("errorCode") String errorCode,
            @JsonProperty("errorMessage") String errorMessage,
            @JsonProperty("retryable") boolean retryable,
            @JsonProperty("attempts") int attempts,
            @JsonProperty("sentAtMs") long sentAtMs) {
    }
}
