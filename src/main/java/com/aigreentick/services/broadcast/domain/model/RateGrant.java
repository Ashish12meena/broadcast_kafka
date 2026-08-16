package com.aigreentick.services.broadcast.domain.model;

/**
 * The answer to "may I send some messages right now?".
 *
 * <p>Grants are deliberately partial: a request for 100 may return 37. The caller sends those 37
 * immediately rather than waiting for a full allocation, which is what keeps the pipeline
 * continuous instead of advancing in stop-start windows.
 *
 * @param granted     how many sends are permitted now; zero means none
 * @param waitMicros  when {@code granted} is zero, how long until the next token exists, so the
 *                    caller can sleep exactly that long instead of polling
 */
public record RateGrant(int granted, long waitMicros) {

    private static final RateGrant NONE = new RateGrant(0, 1_000);

    public static RateGrant of(int granted, long waitMicros) {
        return new RateGrant(granted, waitMicros);
    }

    public static RateGrant none(long waitMicros) {
        return waitMicros <= 0 ? NONE : new RateGrant(0, waitMicros);
    }

    public boolean isEmpty() {
        return granted <= 0;
    }
}
