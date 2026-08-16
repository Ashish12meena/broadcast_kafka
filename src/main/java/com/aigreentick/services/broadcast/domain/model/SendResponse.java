package com.aigreentick.services.broadcast.domain.model;

/**
 * The result of one call to Meta's messages endpoint.
 *
 * <p>{@code transportFailure} distinguishes "Meta answered and refused" from "Meta could not be
 * reached". Only the latter should influence a circuit breaker: a number whose recipients are
 * mostly invalid produces a stream of 4xx responses, and tripping a breaker on those would stop a
 * number that is working perfectly well.
 */
public record SendResponse(
        boolean success,
        String providerMessageId,
        String messageStatus,
        Integer errorCode,
        String errorMessage,
        boolean transportFailure) {

    public static SendResponse accepted(String providerMessageId, String messageStatus) {
        return new SendResponse(true, providerMessageId, messageStatus, null, null, false);
    }

    public static SendResponse rejected(Integer errorCode, String errorMessage) {
        return new SendResponse(false, null, null, errorCode, errorMessage, false);
    }

    public static SendResponse unreachable(String errorMessage) {
        return new SendResponse(false, null, null, null, errorMessage, true);
    }
}
