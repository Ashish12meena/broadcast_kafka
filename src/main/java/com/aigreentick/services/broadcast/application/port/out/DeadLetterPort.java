package com.aigreentick.services.broadcast.application.port.out;

/**
 * Where messages that cannot be processed at all are set aside.
 *
 * <p>A malformed event must not be silently discarded — that loses up to a full batch of recipients
 * with no record that they ever existed — and must not be reprocessed forever either.
 */
public interface DeadLetterPort {

    void send(String rawPayload, String reason, String sourceTopic, int partition, long offset);
}
