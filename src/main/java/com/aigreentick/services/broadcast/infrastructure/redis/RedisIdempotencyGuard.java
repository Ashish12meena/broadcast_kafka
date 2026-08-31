package com.aigreentick.services.broadcast.infrastructure.redis;

import com.aigreentick.services.broadcast.application.port.out.IdempotencyPort;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Stops the same recipient being sent to twice after a redelivery.
 *
 * <p>Meta's messages endpoint has no idempotency key, so the claim has to be taken before the call.
 * Deduplicating afterwards on the returned wamid protects the database but not the customer, who has
 * already received a second copy.
 *
 * <h2>The trade-off, stated plainly</h2>
 * A crash between claiming and sending means that recipient is skipped when Kafka redelivers the
 * batch, and is recovered later by the Messaging Service's stuck-processing cleanup. That is a
 * delayed message. The alternative arrangement produces a duplicate message, which cannot be
 * recovered at all. This implementation prefers the delay, and the choice is configurable via
 * {@code broadcast.idempotency.enabled} because it is a product decision rather than a technical one.
 *
 * <h2>Failing open when Redis is down</h2>
 * If the claim cannot be taken, the send proceeds. Refusing to send during a Redis outage would stop
 * the platform to prevent a duplicate that only occurs on the much rarer redelivery path.
 */
@Component
public class RedisIdempotencyGuard implements IdempotencyPort {

    private static final Logger log = LoggerFactory.getLogger(RedisIdempotencyGuard.class);

    private static final String CLAIMED = "CLAIMED";

    private final StringRedisTemplate redis;
    private final BroadcastProperties properties;

    public RedisIdempotencyGuard(StringRedisTemplate redis, BroadcastProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    @Override
    public boolean claim(Long recipientId) {
        if (!properties.idempotency().enabled() || recipientId == null) {
            return true;
        }
        try {
            Boolean acquired = redis.opsForValue().setIfAbsent(
                    RedisKeys.sentClaim(recipientId), CLAIMED, properties.idempotency().claimTtl());
            return Boolean.TRUE.equals(acquired);

        } catch (DataAccessException e) {
            log.warn("Could not claim recipientId={} reason={}; proceeding with send", recipientId, e.toString());
            return true;
        }
    }

    @Override
    public void release(Long recipientId) {
        if (!properties.idempotency().enabled() || recipientId == null) {
            return;
        }
        try {
            redis.delete(RedisKeys.sentClaim(recipientId));
        } catch (DataAccessException e) {
            // The claim expires on its own. Losing this only delays a legitimate retry.
            log.debug("Could not release claim recipientId={} reason={}", recipientId, e.toString());
        }
    }

    @Override
    public String claimedMessageId(Long recipientId) {
        if (!properties.idempotency().enabled() || recipientId == null) {
            return null;
        }
        try {
            String value = redis.opsForValue().get(RedisKeys.sentClaim(recipientId));
            // CLAIMED means the claim was taken but confirm() never ran — an in-flight send, or one
            // that failed before it got a wamid. Either way there is no message id to report.
            return CLAIMED.equals(value) ? null : value;

        } catch (DataAccessException e) {
            log.debug("Could not read claim recipientId={} reason={}", recipientId, e.toString());
            return null;
        }
    }

    @Override
    public void confirm(Long recipientId, String providerMessageId) {
        if (!properties.idempotency().enabled() || recipientId == null || providerMessageId == null) {
            return;
        }
        try {
            redis.opsForValue().set(
                    RedisKeys.sentClaim(recipientId), providerMessageId, properties.idempotency().claimTtl());
        } catch (DataAccessException e) {
            log.debug("Could not confirm claim recipientId={} reason={}", recipientId, e.toString());
        }
    }
}