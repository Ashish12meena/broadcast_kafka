package com.aigreentick.services.broadcast.infrastructure.meta.simulator;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Map;

/**
 * Settings for the test-profile simulator, under {@code broadcast.simulator.*}.
 *
 * <p>Only the callback endpoint is configured here. Everything that identifies a send —
 * {@code phoneNumberId}, {@code wabaAccountId}, the recipient — comes from the {@code DispatchEvent}
 * that produced it, so there is nothing per-number to keep in step with the batches arriving on
 * Kafka.
 *
 * @param callbackUrl     absolute URL that receives the simulated status callbacks. Blank disables
 *                        them: sends are still simulated and one warning is logged
 * @param headers         optional headers, for a receiver behind a verification token or API key
 * @param minDelay        lower bound of the random gap before each status
 * @param maxDelay        upper bound of that gap
 * @param maxConnections  size of the simulator's own connection pool. Separate from
 *                        {@code broadcast.meta.max-connections} so a slow callback receiver cannot
 *                        starve the send path. Keep it at or above {@code maxInFlight} — raising
 *                        in-flight past the pool just moves the failure into Reactor Netty's
 *                        pending-acquire queue, where it surfaces as rejected requests rather than
 *                        as waiting ones
 * @param maxInFlight     how many callbacks may be POSTING at once. Counts requests only: a callback
 *                        waiting out its delay does not occupy a slot, because the wait happens in
 *                        an earlier, unbounded stage of the pipeline. It did once, which capped the
 *                        whole simulator at roughly {@code maxInFlight / meanDelaySeconds} callbacks
 *                        per second — about three and a half at the old defaults, against the two
 *                        hundred and forty a standard-tier broadcast produces
 * @param responseTimeout how long to wait for the receiver before abandoning a callback. The
 *                        important one — an untimed request holds its connection forever, so a
 *                        hung receiver wedges the pool permanently instead of briefly
 * @param connectTimeout  how long to wait for the TCP connection itself
 */
@ConfigurationProperties(prefix = "broadcast.simulator")
public record MetaSimulatorProperties(
        String callbackUrl,
        Map<String, String> headers,
        Duration minDelay,
        Duration maxDelay,
        Integer maxConnections,
        Integer maxInFlight,
        Duration responseTimeout,
        Duration connectTimeout) {

    public MetaSimulatorProperties {
        callbackUrl = callbackUrl == null ? "" : callbackUrl.trim();
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        // Short by default. The randomness is the point — it makes messages overtake each other
        // and exercises the receiver's out-of-order handling — but the magnitude buys nothing
        // except waiting. Seconds-long defaults made a broadcast take minutes to produce statuses
        // that a real Meta account returns in well under one.
        minDelay = minDelay == null ? Duration.ofMillis(200) : minDelay;
        maxDelay = maxDelay == null ? Duration.ofSeconds(2) : maxDelay;

        // Both raised from 32, and raised together. 128 concurrent posts against a receiver
        // answering in tens of milliseconds clears the ~240 callbacks per second that Meta's
        // standard 80 mps tier implies, with headroom for the 1000 mps tier's bursts to queue
        // in the sink rather than be dropped.
        maxConnections = maxConnections == null ? 128 : maxConnections;
        maxInFlight = maxInFlight == null ? 128 : maxInFlight;
        responseTimeout = responseTimeout == null ? Duration.ofSeconds(10) : responseTimeout;
        connectTimeout = connectTimeout == null ? Duration.ofSeconds(3) : connectTimeout;

        if (maxDelay.compareTo(minDelay) < 0) {
            throw new IllegalArgumentException(
                    "broadcast.simulator.max-delay must be >= min-delay");
        }
        if (maxConnections < 1) {
            throw new IllegalArgumentException(
                    "broadcast.simulator.max-connections must be >= 1");
        }
        if (maxInFlight < 1) {
            throw new IllegalArgumentException(
                    "broadcast.simulator.max-in-flight must be >= 1");
        }
        if (maxInFlight > maxConnections) {
            // Rejected rather than clamped. This configuration does not fail loudly at runtime — it
            // overflows the pending-acquire queue and drops callbacks, which reads as a slow
            // simulator rather than a misconfigured one. Better to refuse to start.
            throw new IllegalArgumentException(
                    "broadcast.simulator.max-in-flight (%d) must not exceed max-connections (%d); "
                            .formatted(maxInFlight, maxConnections)
                            + "the excess would be rejected by the connection pool, not queued");
        }
    }

    public boolean callbacksEnabled() {
        return !callbackUrl.isEmpty();
    }
}