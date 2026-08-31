package com.aigreentick.services.broadcast.application.port.out;

/**
 * Guards against sending the same recipient twice.
 *
 * <p>Meta's messages endpoint accepts no idempotency key, so duplicate suppression has to happen
 * before the call rather than being reconciled after it. Deduplicating on receipt protects the
 * database; it does not protect the customer, who has already received two messages.
 */
public interface IdempotencyPort {

    /** @return true if this caller may send to the recipient; false if someone already has */
    boolean claim(Long recipientId);

    /** Releases a claim whose send failed in a way that should be retried. */
    void release(Long recipientId);

    /** Marks the recipient as sent and records the wamid against the claim. */
    void confirm(Long recipientId, String providerMessageId);

    /**
     * The provider message id stored against an existing claim, if there is one.
     *
     * <p>Exists so a suppressed duplicate can still report an outcome. Without it the only honest
     * thing to report is "sent, wamid unknown", and the Messaging Service would record a message
     * whose delivery receipts can never be matched.
     *
     * @return the wamid {@link #confirm} stored, or null when the claim is still the bare marker,
     *         has expired, or the store is unreachable
     */
    String claimedMessageId(Long recipientId);
}