package com.aigreentick.services.broadcast.domain.policy;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * How long to wait before re-attempting a send.
 *
 * <p>Full jitter rather than plain exponential backoff: a batch of a thousand recipients that all
 * fail at the same instant would otherwise all retry at the same instant, converting one failure
 * into a synchronised burst.
 *
 * <p>Note that jitter is the second line of defence here, not the first. Retries acquire tokens from
 * the same bucket as first attempts, so a number cannot exceed its rate by retrying no matter how
 * the delays fall.
 */
public final class RetryPolicy {

    private final Duration base;
    private final Duration max;
    private final int maxAttempts;

    public RetryPolicy(Duration base, Duration max, int maxAttempts) {
        this.base = base;
        this.max = max;
        this.maxAttempts = maxAttempts;
    }

    public boolean shouldRetry(int attemptsSoFar) {
        return attemptsSoFar < maxAttempts;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    /** Full-jitter delay: a uniform draw from {@code [0, min(max, base * 2^attempt))}. */
    public Duration delayFor(int attemptsSoFar) {
        long exponential = base.toMillis() << Math.min(attemptsSoFar, 16);
        long ceiling = Math.min(exponential, max.toMillis());
        long jittered = ceiling <= 0 ? 0 : ThreadLocalRandom.current().nextLong(ceiling);
        return Duration.ofMillis(jittered);
    }
}
