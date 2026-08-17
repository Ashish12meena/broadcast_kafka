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
 * @param callbackUrl absolute URL that receives the simulated status callbacks. Blank disables them:
 *                    sends are still simulated and one warning is logged
 * @param headers     optional headers, for a receiver behind a verification token or API key
 * @param minDelay    lower bound of the random gap before each status
 * @param maxDelay    upper bound of that gap
 */
@ConfigurationProperties(prefix = "broadcast.simulator")
public record MetaSimulatorProperties(
        String callbackUrl,
        Map<String, String> headers,
        Duration minDelay,
        Duration maxDelay) {

    public MetaSimulatorProperties {
        callbackUrl = callbackUrl == null ? "" : callbackUrl.trim();
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        minDelay = minDelay == null ? Duration.ofSeconds(1) : minDelay;
        maxDelay = maxDelay == null ? Duration.ofSeconds(8) : maxDelay;

        if (maxDelay.compareTo(minDelay) < 0) {
            throw new IllegalArgumentException(
                    "broadcast.simulator.max-delay must be >= min-delay");
        }
    }

    public boolean callbacksEnabled() {
        return !callbackUrl.isEmpty();
    }
}
