package com.aigreentick.services.broadcast.application.port.out;

import com.aigreentick.services.broadcast.domain.model.BatchResult;

import java.util.concurrent.CompletableFuture;

/**
 * Reports outcomes back to the Messaging Service.
 *
 * <p>This is the one piece of state the service cannot afford to drop: a lost result leaves a
 * message row stuck mid-flight, and the recovery path for that is to send it to the customer a
 * second time.
 *
 * <h2>Asynchronous, because the caller is a send thread</h2>
 * {@link #publishAsync} returns a future rather than blocking. The caller is
 * {@code ResultCollector.record}, running on a virtual thread that has just finished a Meta call
 * and should not then wait on a broker round trip. The future is what {@code completeBatch} joins
 * before it lets the Kafka offset move — which is how "every recipient has an outcome" and "every
 * outcome is durable" became the same statement instead of two separated by a flush interval.
 */
public interface ResultPublisherPort {

    /**
     * Publishes and returns immediately.
     *
     * @return a future that completes when the broker has acknowledged, or fails if it did not.
     *         The failure must reach {@code completeBatch}, because an unacknowledged result means
     *         the Kafka offset for its batch must not be committed
     */
    CompletableFuture<Void> publishAsync(BatchResult result);

    /**
     * Publishes and waits.
     *
     * <p>Retained for callers with nothing useful to do while they wait — the shutdown path, and
     * tests. Not for the send path.
     */
    default void publish(BatchResult result) {
        publishAsync(result).join();
    }
}
