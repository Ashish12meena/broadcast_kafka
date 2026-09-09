package com.aigreentick.services.broadcast.infrastructure.observability;

import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Logs a degradation once when it begins and once when it ends, never once per occurrence.
 *
 * <h2>Why this exists</h2>
 * {@code DispatchWorker.run()} calls the rate limiter in a loop with no sleep on the fallback path,
 * because a fallback grant is non-empty and the loop drains it immediately. A {@code log.warn} in
 * that path therefore fires as fast as the CPU allows, per active phone number, for the whole
 * duration of a Redis outage. That is unbounded log volume produced exactly when the log pipeline
 * is the thing you need working.
 *
 * <p>The same shape applies per-recipient in the idempotency guard and per-read in the capacity
 * store. All three take the same treatment: two log lines per outage, and a counter carrying the
 * real rate.
 *
 * <h2>The rule</h2>
 * A log line describes a <em>state change</em>. A metric describes a <em>rate</em>. Anything that
 * can happen thousands of times per second belongs in the second category.
 *
 * <p>Not thread-confined: the dispatch loop runs one worker per phone number and the send path runs
 * on virtual threads, so the flag has to be atomic. {@code compareAndSet} also gives the exactly-once
 * guarantee on the transition itself, which is the whole point.
 */
public final class RedisDegradationTracker {

    private final Logger log;
    private final String subsystem;
    private final AtomicBoolean degraded = new AtomicBoolean(false);

    public RedisDegradationTracker(Logger log, String subsystem) {
        this.log = log;
        this.subsystem = subsystem;
    }

    /**
     * Marks the subsystem degraded. Logs only on the false-to-true transition.
     *
     * @param reason short cause, typically {@code e.toString()} — not a stack trace, because the
     *               interesting information is that Redis is unreachable, not the frame it was
     *               noticed in
     */
    public void enter(String reason) {
        if (degraded.compareAndSet(false, true)) {
            log.warn("Degraded: {} is running without Redis; local fallback engaged reason={}",
                    subsystem, reason);
        }
    }

    /** Marks the subsystem healthy. Logs only on the true-to-false transition. */
    public void exit() {
        if (degraded.compareAndSet(true, false)) {
            log.info("Recovered: {} is using Redis again; local fallback disengaged", subsystem);
        }
    }

    /** Exposed for the health indicator and for tests. */
    public boolean isDegraded() {
        return degraded.get();
    }
}
