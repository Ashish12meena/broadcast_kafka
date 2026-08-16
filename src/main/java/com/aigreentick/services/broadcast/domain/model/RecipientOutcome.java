package com.aigreentick.services.broadcast.domain.model;

/**
 * What happened to one recipient, as reported back to the Messaging Service.
 *
 * <p>{@code success} means Meta accepted the message, not that it was delivered — delivery arrives
 * later as a webhook keyed on {@code providerMessageId}. That wamid is the only link between this
 * send and every future status update for it, and it cannot be recovered if it is not recorded now.
 */
public record RecipientOutcome(
        Long recipientId,
        Long messageId,
        Long contactId,
        boolean success,
        String providerMessageId,
        String messageStatus,
        String errorCode,
        String errorMessage,
        boolean retryable,
        int attempts,
        long sentAtMs) {

    public static RecipientOutcome accepted(
            Recipient recipient, String providerMessageId, String messageStatus, int attempts) {
        return new RecipientOutcome(
                recipient.recipientId(), recipient.messageId(), recipient.contactId(),
                true, providerMessageId, messageStatus, null, null, false, attempts,
                System.currentTimeMillis());
    }

    public static RecipientOutcome failed(
            Recipient recipient, String errorCode, String errorMessage, boolean retryable, int attempts) {
        return new RecipientOutcome(
                recipient.recipientId(), recipient.messageId(), recipient.contactId(),
                false, null, null, errorCode, errorMessage, retryable, attempts,
                System.currentTimeMillis());
    }
}
