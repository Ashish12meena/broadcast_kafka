package com.aigreentick.services.broadcast.infrastructure.redis;

import com.aigreentick.services.broadcast.common.constants.DomainConstants;
import com.aigreentick.services.broadcast.common.constants.InfraConstants;
import com.aigreentick.services.broadcast.application.port.out.RateLimiterPort;
import com.aigreentick.services.broadcast.domain.model.RateGrant;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import com.aigreentick.services.broadcast.infrastructure.observability.BroadcastMetrics;
import com.aigreentick.services.broadcast.infrastructure.observability.RedisDegradationTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The shared meter, backed by a Redis token bucket.
 *
 * <p>This is what makes the per-number limit correct across instances. Two instances each wanting
 * 300 sends against a 500 mps number draw from the same bucket, so they receive 300 and 200. Neither
 * knows the other exists and no instance count is configured anywhere — adding a third replica
 * changes nothing about correctness.
 *
 * <p>A local semaphore, however carefully sized, cannot do this: it is correct on one instance and
 * wrong on two.
 *
 * <h2>A note on logging in here</h2>
 * {@code acquire} is called from {@code DispatchWorker.run()}, which is a loop with no sleep on the
 * fallback path — a fallback grant is non-empty, so the loop drains it and comes straight back. Any
 * per-call log statement in this class therefore fires at loop frequency, per active phone number,
 * for the entire duration of a Redis outage. Every failure path below logs the transition and
 * increments a counter instead; the counter is what tells you the rate.
 */
@Component
@Primary 
public class RedisRateLimiter implements RateLimiterPort {

    private static final Logger log = LoggerFactory.getLogger(RedisRateLimiter.class);

    private final StringRedisTemplate redis;
    private final RedisScript<List> tokenBucketScript;
    private final BroadcastProperties properties;
    private final LocalFallbackRateLimiter fallback;
    private final BroadcastMetrics metrics;

    private final RedisDegradationTracker degradation =
            new RedisDegradationTracker(log, "rate limiting");

    /**
     * Numbers already warned about for missing capacity. Unlike a Redis outage this condition does
     * not clear on its own — it persists until the control plane publishes capacity — so without
     * this set a single unregistered number logs on every loop iteration indefinitely.
     */
    private final Set<String> warnedUnknownCapacity = ConcurrentHashMap.newKeySet();

    public RedisRateLimiter(
            StringRedisTemplate redis,
            @SuppressWarnings("rawtypes") RedisScript<List> tokenBucketScript,
            BroadcastProperties properties,
            LocalFallbackRateLimiter fallback,
            BroadcastMetrics metrics) {
        this.redis = redis;
        this.tokenBucketScript = tokenBucketScript;
        this.properties = properties;
        this.fallback = fallback;
        this.metrics = metrics;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RateGrant acquire(String phoneNumberId, int requested) {
        if (requested <= 0) {
            return RateGrant.none(0);
        }
        metrics.tokensRequested(phoneNumberId, requested);

        try {
            List<Long> result = redis.execute(
                    tokenBucketScript,
                    List.of(RedisKeys.tokenBucket(phoneNumberId), RedisKeys.capacity(phoneNumberId)),
                    String.valueOf(System.currentTimeMillis() * DomainConstants.Dispatch.MICROS_PER_MILLI),
                    String.valueOf(requested),
                    String.valueOf(properties.rateLimit().burstSeconds()),
                    String.valueOf(InfraConstants.Redis.TOKEN_BUCKET_TTL_SECONDS));

            if (result == null || result.size() < InfraConstants.Redis.TOKEN_BUCKET_RESULT_SIZE) {
                // Redis answered but the script did not return what it should. Same treatment as an
                // outage: it repeats at loop frequency until whatever is wrong is fixed.
                degradation.enter("token bucket script returned no usable result");
                metrics.rateLimiterDegraded(phoneNumberId);
                return fallbackGrant(phoneNumberId, requested);
            }

            long granted = result.get(InfraConstants.Redis.TOKEN_BUCKET_RESULT_GRANTED_INDEX);
            long waitMicros = result.get(InfraConstants.Redis.TOKEN_BUCKET_RESULT_WAIT_INDEX);

            if (granted == InfraConstants.Redis.CAPACITY_UNKNOWN) {
                // Redis is healthy but the Messaging Service has never published capacity for this
                // number. Worth a warning: it means a batch is being dispatched from a number the
                // control plane does not know about.
                if (warnedUnknownCapacity.add(phoneNumberId)) {
                    log.warn("No capacity published for phoneNumberId={}; using default rate until the "
                            + "control plane publishes one", phoneNumberId);
                }
                metrics.capacityUnknown(phoneNumberId);
                return fallbackGrant(phoneNumberId, requested);
            }

            // Reached only on a healthy round trip, so this is the correct place to close out a
            // degradation. Both calls are compareAndSet-guarded and cost a single atomic read when
            // there is nothing to change, which is the overwhelmingly common case.
            degradation.exit();
            warnedUnknownCapacity.remove(phoneNumberId);

            metrics.tokensGranted(phoneNumberId, (int) granted);
            return granted > 0
                    ? RateGrant.of((int) granted, 0)
                    : RateGrant.none(waitMicros);

        } catch (DataAccessException | IllegalStateException e) {
            // Never fail open. Under-sending delays a campaign; over-sending earns rate limits that
            // lower the number's quality rating, which lowers its throughput tier — a much more
            // expensive and much slower failure to undo.
            degradation.enter(e.toString());
            metrics.rateLimiterDegraded(phoneNumberId);
            return fallbackGrant(phoneNumberId, requested);
        }
    }

    private RateGrant fallbackGrant(String phoneNumberId, int requested) {
        RateGrant grant = fallback.acquire(phoneNumberId, requested);
        metrics.tokensGranted(phoneNumberId, grant.granted());
        return grant;
    }

    /** Exposed for {@code CapacityHealthIndicator} and for tests. */
    public boolean isDegraded() {
        return degradation.isDegraded();
    }
}
