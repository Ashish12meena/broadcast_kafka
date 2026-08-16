package com.aigreentick.services.broadcast.infrastructure.kafka.event;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * What the Messaging Service publishes on the outbound topic.
 *
 * <p>{@code phoneNumberId} is Meta's string identifier and {@code wabaAccountId} is the platform's
 * numeric one. They are different values for different things and must never be substituted for one
 * another — using the account identifier in the Graph API path produces a total failure against a
 * number that looks correctly configured.
 *
 * <p>Snake-case aliases are accepted alongside camel case so a producer-side naming change cannot
 * silently drop a field. Unknown fields are ignored so the producer can add one without a coordinated
 * release.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DispatchEvent(
        @JsonAlias("campaign_id") Long campaignId,
        @JsonAlias("phone_number_id") String phoneNumberId,
        @JsonAlias("waba_account_id") Long wabaAccountId,
        @JsonAlias("access_token") String accessToken,
        List<PayloadItem> payloads) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PayloadItem(
            @JsonAlias("recipient_id") Long recipientId,
            @JsonAlias("message_id") Long messageId,
            @JsonAlias("contact_id") Long contactId,
            /** The complete Meta request body, rendered upstream and forwarded verbatim. */
            @JsonAlias("request_payload") String requestPayload) {
    }

    /** @return null when the event is usable, otherwise why it is not */
    public String validationError() {
        if (campaignId == null) {
            return "campaignId is missing";
        }
        if (phoneNumberId == null || phoneNumberId.isBlank()) {
            return "phoneNumberId is missing";
        }
        if (accessToken == null || accessToken.isBlank()) {
            return "accessToken is missing";
        }
        if (payloads == null || payloads.isEmpty()) {
            return "payloads is empty";
        }
        boolean anyPayloadMissing = payloads.stream()
                .anyMatch(item -> item.requestPayload() == null || item.requestPayload().isBlank());
        if (anyPayloadMissing) {
            return "one or more payloads have no request body";
        }
        return null;
    }
}
