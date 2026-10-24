package com.aigreentick.services.broadcast.application.port.out;

/**
 * Where messages that cannot be processed at all are set aside.
 *
 * <p>A malformed event must not be silently discarded — that loses up to a full batch of recipients
 * with no record that they ever existed — and must not be reprocessed forever either.
 */
public interface DeadLetterPort {

    /**
     * @return true once the broker has the message; false if it could not be written, in which
     *         case the caller must not acknowledge the source record
     */
    boolean send(String rawPayload, String reason, String sourceTopic, int partition, long offset);
}
