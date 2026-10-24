package com.aigreentick.services.broadcast.domain.model;

/**
 * The result of one call to Meta's messages endpoint.
 *
 * <p>{@code transportFailure} distinguishes "Meta answered and refused" from "Meta could not be
 * reached". Only the latter should influence a circuit breaker: a number whose recipients are
 * mostly invalid produces a stream of 4xx responses, and tripping a breaker on those would stop a
 * number that is working perfectly well.
 *
 * <p>{@code outcomeUnknown} marks a transport failure after the request was sent (read timeout,
 * connection reset mid-response). Meta may have accepted the message, and it has no idempotency
 * key, so such a send must not be retried.
 */
public record SendResponse(
        boolean success,
        String providerMessageId,
        String messageStatus,
        Integer errorCode,
        String errorMessage,
        boolean transportFailure,
        boolean outcomeUnknown) {

    public static SendResponse accepted(String providerMessageId, String messageStatus) {
        return new SendResponse(true, providerMessageId, messageStatus, null, null, false, false);
    }

    public static SendResponse rejected(Integer errorCode, String errorMessage) {
        return new SendResponse(false, null, null, errorCode, errorMessage, false, false);
    }

    /** Meta was never reached; the request did not leave, so a retry is safe. */
    public static SendResponse unreachable(String errorMessage) {
        return new SendResponse(false, null, null, null, errorMessage, true, false);
    }

    /** The request was sent but no answer came back; Meta may have delivered it. */
    public static SendResponse outcomeUnknown(String errorMessage) {
        return new SendResponse(false, null, null, null, errorMessage, true, true);
    }
}
