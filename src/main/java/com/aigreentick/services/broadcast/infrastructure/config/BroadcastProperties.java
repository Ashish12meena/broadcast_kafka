package com.aigreentick.services.broadcast.infrastructure.config;

import com.aigreentick.services.broadcast.common.constants.InfraConstants;
import jakarta.validation.constraints.AssertTrue;
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
@ConfigurationProperties(prefix = InfraConstants.ConfigKeys.BROADCAST_PREFIX)
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
     * @param depthTtl                   how long a published queue-depth reading survives in Redis.
     *                                   <p>Explicit, and deliberately NOT derived from
     *                                   {@code maxSleep}. It used to be {@code maxSleep x 10} —
     *                                   two seconds — on the reasoning that this was "several times
     *                                   the publish interval". {@code maxSleep} is not a publish
     *                                   interval; it is how long a worker sleeps when the token
     *                                   bucket denies it. While a worker loops it republishes on
     *                                   every pass, so two seconds never expired. The moment a
     *                                   worker drained its queue and exited, nothing renewed the
     *                                   key, it vanished two seconds later, and the Messaging
     *                                   Service's next poll found nothing. Every claim then fell
     *                                   back to {@code fallback-claim-size}, which drained fast,
     *                                   exited again, and locked both services into a cycle where
     *                                   the reading was always absent exactly when it was read.
     *                                   <p>Keep this comfortably longer than the reader's
     *                                   {@code messaging.campaign.dispatch.depth-stale-after}, so
     *                                   that staleness is decided by the timestamp embedded in the
     *                                   value rather than by the key expiring. The reader already
     *                                   has that check; a short TTL is what stopped it ever running
     * @param housekeepingInterval       how often the scheduler refreshes every known number's
     *                                   depth key and evicts queues that have gone quiet. Must stay
     *                                   well below {@code depthTtl}
     */
    public record Dispatch(
            @Min(1) int chunkSize,
            @Min(1) int maxConcurrentSends,
            @Min(1) int maxQueuedBatchesPerNumber,
            @Min(1) int queueResumeThreshold,
            Duration maxSleep,
            Duration depthTtl,
            Duration shutdownGrace,
            Duration housekeepingInterval) {

        /**
         * Defaults {@code depthTtl} so a profile that replaces the whole {@code dispatch} block
         * cannot silently reintroduce a short-lived key.
         */
        public Dispatch {
            depthTtl = depthTtl == null ? DEFAULT_DEPTH_TTL : depthTtl;
        }

        private static final Duration DEFAULT_DEPTH_TTL = Duration.ofSeconds(60);

        /**
         * Refuses to start on the combination that caused the original fault: a TTL short enough
         * that a key can expire between two refreshes. Four times is arbitrary but generous; the
         * point is that the failure is a startup error rather than a throughput number nobody can
         * explain.
         */
        @AssertTrue(message = "broadcast.dispatch.depth-ttl must be at least 4x "
                + "broadcast.dispatch.housekeeping-interval, or a number's queue-depth key expires "
                + "between refreshes and the Messaging Service falls back to fallback-claim-size "
                + "on every claim")
        public boolean isDepthTtlAboveRefreshInterval() {
            return housekeepingInterval == null
                    || depthTtl.compareTo(housekeepingInterval.multipliedBy(4)) >= 0;
        }
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