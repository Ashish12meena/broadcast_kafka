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
 *                        starve the send path
 * @param maxInFlight     how many callbacks may be posting at once. This is the real backpressure
 *                        control: without it, a large broadcast queues three requests per recipient
 *                        against a pool of tens, and the overflow is rejected rather than delayed
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
        minDelay = minDelay == null ? Duration.ofSeconds(1) : minDelay;
        maxDelay = maxDelay == null ? Duration.ofSeconds(8) : maxDelay;

        maxConnections = maxConnections == null ? 32 : maxConnections;
        maxInFlight = maxInFlight == null ? 32 : maxInFlight;
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
    }

    public boolean callbacksEnabled() {
        return !callbackUrl.isEmpty();
    }
}