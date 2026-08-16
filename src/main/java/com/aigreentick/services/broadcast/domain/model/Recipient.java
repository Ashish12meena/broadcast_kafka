package com.aigreentick.services.broadcast.domain.model;

/**
 * One recipient of a broadcast, with its Meta request body already rendered by the Messaging
 * Service. This service never builds or inspects the payload — it forwards it verbatim.
 */
public record Recipient(
        Long recipientId,
        Long messageId,
        Long contactId,
        String requestPayload) {
}
