package com.aigreentick.services.broadcast.infrastructure.kafka.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * One batch of recipients to send, as it arrives on {@code whatsapp.messages.outbound}.
 *
 * <h2>{@code accessToken} is a live credential arriving over Kafka</h2>
 * It is a known problem, not a design choice. The token is at rest on the topic for its whole
 * retention period, replicated to every broker, and readable by anything with consumer rights.
 *
 * <p>Moving the fetch to this side is not a one-field change: this service has no internal HTTP
 * client, no service-to-service credentials, and no organization or project context on this path,
 * so resolving the token here means giving it all three — an authorization decision about whether
 * the WABA service should trust this service directly. Until that is made, restrict the topic's
 * ACLs, shorten its retention, and rotate on the assumption the tokens have been readable.
 *
 * <h2>{@code traceId} is new and is not optional</h2>
 * Both services logged a campaign id and neither logged anything tying one <em>run</em> together
 * across the Kafka hop — which is precisely where messages go missing. This is the field that makes
 * "where did the other two hundred go" a single grep. It is propagated into MDC on receipt and back
 * out on the result event.
 *
 * @param validationError set by the producer when it knows the batch is unsendable; the listener
 *                        dead-letters rather than attempting it
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DispatchEvent(
        Long campaignId,
        String phoneNumberId,
        Long wabaAccountId,
        String accessToken,
        String traceId,
        List<Payload> payloads,
        String validationError) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payload(
            Long recipientId,
            Long messageId,
            Long contactId,
            String requestPayload) {
    }

    public String validationError() {
        return validationError;
    }

    public int size() {
        return payloads == null ? 0 : payloads.size();
    }
}
