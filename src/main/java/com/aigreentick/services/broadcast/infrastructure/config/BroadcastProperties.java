package com.aigreentick.services.broadcast.infrastructure.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * All tuning for the service, under {@code broadcast.*}.
 *
 * <p>Nothing about a phone number's throughput appears here. Capacity arrives at runtime from the
 * Messaging Service, so a tier change takes effect without a redeploy — which is the whole point of
 * the design. What lives here is how this instance behaves: chunk sizes, pool sizes, timeouts.
 */
@Validated
@ConfigurationProperties(prefix = "broadcast")
public record BroadcastProperties(
        Dispatch dispatch,
        RateLimit rateLimit,
        Meta meta,
        Retry retry,
        CircuitBreaker circuitBreaker,
        Idempotency idempotency,
        Results results,
        Topics topics) {

    /**
     * @param chunkSize                  tokens requested per acquisition. Larger means fewer Redis
     *                                   round trips and coarser pacing; 50 is a reasonable balance
     *                                   at every tier from 80 to 1,000 mps
     * @param maxConcurrentSends         local in-flight ceiling for this instance. Sized from
     *                                   {@code mps x p99 latency} and never above the HTTP
     *                                   connection pool, or requests simply queue inside the pool
     * @param maxQueuedBatchesPerNumber  when a number's queue reaches this, the Kafka consumer is
     *                                   paused. This is the backpressure that stops a slow Meta from
     *                                   being absorbed into heap
     * @param queueResumeThreshold       queue depth at which consumption resumes. Deliberately well
     *                                   below the pause threshold so the consumer does not oscillate
     * @param maxSleep                   longest a worker sleeps waiting for tokens before looking
     *                                   again, so capacity increases are picked up promptly
     */
    public record Dispatch(
            @Min(1) int chunkSize,
            @Min(1) int maxConcurrentSends,
            @Min(1) int maxQueuedBatchesPerNumber,
            @Min(1) int queueResumeThreshold,
            Duration maxSleep,
            Duration shutdownGrace,
            Duration housekeepingInterval) {
    }

    /**
     * @param burstSeconds      how much unused capacity a number may bank. Small on purpose: an idle
     *                          number that accumulated a large burst would spend it all at once the
     *                          moment a campaign starts, which is precisely what triggers a 429
     * @param defaultMps        assumed rate when nothing is known about a number. Never set this to
     *                          the high tier — guessing low costs time, guessing high costs the
     *                          number's quality rating, which then lowers the real ceiling
     * @param fallbackFraction  proportion of last-known capacity used while Redis is unreachable
     * @param assumedInstances  divisor for the fallback rate. Over-estimating is the safe direction
     */
    public record RateLimit(
            double burstSeconds,
            @Min(1) int defaultMps,
            double fallbackFraction,
            @Min(1) int assumedInstances,
            Duration capacityCacheTtl,
            Duration degradeLockTtl,
            Duration upgradeBackoff) {
    }

    public record Meta(
            @NotBlank String baseUrl,
            Duration connectTimeout,
            Duration readTimeout,
            @Min(1) int maxConnections,
            Duration pendingAcquireTimeout,
            Duration idleTimeout) {
    }

    public record Retry(
            @Min(1) int maxAttempts,
            Duration baseBackoff,
            Duration maxBackoff) {
    }

    public record CircuitBreaker(
            @Min(1) int failureRateThreshold,
            @Min(1) int slidingWindowSize,
            @Min(1) int minimumNumberOfCalls,
            Duration waitDurationInOpenState) {
    }

    public record Idempotency(boolean enabled, Duration claimTtl) {
    }

    /**
     * @param flushSize    outcomes buffered before publishing. One event per batch rather than per
     *                     recipient, so the Messaging Service can apply them in a single transaction
     * @param flushInterval longest an outcome waits to be published when volume is low
     */
    public record Results(
            @Min(1) int flushSize,
            Duration flushInterval) {
    }

    public record Topics(
            @NotBlank String outboundMessages,
            @NotBlank String capacityUpdates,
            @NotBlank String messageResults,
            @NotBlank String deadLetter) {
    }
}
