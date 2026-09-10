package com.aigreentick.services.broadcast.infrastructure.redis;

import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Publishes how much work this service is still holding for each phone number.
 *
 * <h2>Why the sender reports its own queue depth</h2>
 * It is the input to the credit loop that replaced the Messaging Service's rate arithmetic. That
 * arithmetic — {@code effective_mps x elapsed}, metered from a per-pod map — could not be correct
 * with more than one Messaging pod, and it was duplicating a decision this service already makes
 * properly with a shared Redis token bucket. Removing it leaves Messaging needing exactly one fact
 * it cannot work out for itself: whether there is room downstream.
 *
 * <p>A depth is the right thing to publish rather than a rate. A rate has to be agreed on by two
 * parties and stays wrong until both are redeployed; a depth is an observation, and the party
 * reading it can decide what to do about it without either side knowing the other's configuration.
 *
 * <h2>Written on every drain, with a TTL</h2>
 * The TTL is what makes a dead publisher safe: its key disappears, the reader sees nothing, and the
 * reader falls back to a deliberately modest fixed claim. A key with no TTL would leave a stale
 * depth behind forever, and a stale low depth is an instruction to flood.
 */
@Component
public class RedisQueueDepthPublisher {

    private static final Logger log = LoggerFactory.getLogger(RedisQueueDepthPublisher.class);

    /** Matches {@code RedisQueueDepthAdapter.KEY_PREFIX} in Messaging. Both change together. */
    private static final String KEY_PREFIX = "bcast:depth:";

    private final StringRedisTemplate redis;
    private final Duration ttl;

    public RedisQueueDepthPublisher(StringRedisTemplate redis, BroadcastProperties properties) {
        this.redis = redis;
        // Several times the publish interval, so an ordinary GC pause does not blank the reading
        // and cause an unnecessary fallback, while a genuinely dead pod's key still expires within
        // a poll or two.
        this.ttl = properties.dispatch().maxSleep().multipliedBy(10);
    }

    /**
     * Records the current depth for one number.
     *
     * <p>Depth and timestamp go into one value rather than two keys. The reader checks freshness
     * before trusting the number, and with two keys that check can pass for a depth that came from
     * a different write — a small window, and the wrong side to be wrong on.
     */
    public void publish(String phoneNumberId, int pendingRecipients) {
        try {
            redis.opsForValue().set(
                    KEY_PREFIX + phoneNumberId,
                    pendingRecipients + ":" + Instant.now().toEpochMilli(),
                    ttl);
        } catch (DataAccessException e) {
            // Never fatal. A missing reading costs the sender nothing and costs Messaging a
            // conservative claim; failing a drain over it would stop sending to prevent a
            // throughput dip, which is the wrong trade in every direction.
            log.debug("Could not publish queue depth for phoneNumberId={}", phoneNumberId);
        }
    }
}
