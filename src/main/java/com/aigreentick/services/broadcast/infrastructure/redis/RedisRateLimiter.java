package com.aigreentick.services.broadcast.infrastructure.redis;

import com.aigreentick.services.broadcast.application.port.out.RateLimiterPort;
import com.aigreentick.services.broadcast.domain.model.RateGrant;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import com.aigreentick.services.broadcast.infrastructure.observability.BroadcastMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.List;

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
 */
@Component
@Primary 
public class RedisRateLimiter implements RateLimiterPort {

    private static final Logger log = LoggerFactory.getLogger(RedisRateLimiter.class);

    /** The script's signal that no capacity has been published for a number. */
    private static final long CAPACITY_UNKNOWN = -1L;

    private static final long BUCKET_TTL_SECONDS = 3_600;

    private final StringRedisTemplate redis;
    private final RedisScript<List> tokenBucketScript;
    private final BroadcastProperties properties;
    private final LocalFallbackRateLimiter fallback;
    private final BroadcastMetrics metrics;

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
                    String.valueOf(System.currentTimeMillis() * 1_000L),
                    String.valueOf(requested),
                    String.valueOf(properties.rateLimit().burstSeconds()),
                    String.valueOf(BUCKET_TTL_SECONDS));

            if (result == null || result.size() < 2) {
                log.warn("Token bucket returned no result phoneNumberId={}; using local fallback",
                        phoneNumberId);
                return fallbackGrant(phoneNumberId, requested);
            }

            long granted = result.get(0);
            long waitMicros = result.get(1);

            if (granted == CAPACITY_UNKNOWN) {
                // Redis is healthy but the Messaging Service has never published capacity for this
                // number. Worth a warning: it means a batch is being dispatched from a number the
                // control plane does not know about.
                log.warn("No capacity published for phoneNumberId={}; using default rate", phoneNumberId);
                return fallbackGrant(phoneNumberId, requested);
            }

            metrics.tokensGranted(phoneNumberId, (int) granted);
            return granted > 0
                    ? RateGrant.of((int) granted, 0)
                    : RateGrant.none(waitMicros);

        } catch (DataAccessException | IllegalStateException e) {
            // Never fail open. Under-sending delays a campaign; over-sending earns rate limits that
            // lower the number's quality rating, which lowers its throughput tier — a much more
            // expensive and much slower failure to undo.
            log.warn("Redis unavailable for rate limiting phoneNumberId={} reason={}; using local fallback",
                    phoneNumberId, e.toString());
            return fallbackGrant(phoneNumberId, requested);
        }
    }

    private RateGrant fallbackGrant(String phoneNumberId, int requested) {
        RateGrant grant = fallback.acquire(phoneNumberId, requested);
        metrics.tokensGranted(phoneNumberId, grant.granted());
        return grant;
    }
}
