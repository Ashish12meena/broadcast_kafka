package com.aigreentick.services.broadcast.infrastructure.redis;

import com.aigreentick.services.broadcast.application.port.out.CapacityStorePort;
import com.aigreentick.services.broadcast.domain.model.CapacitySource;
import com.aigreentick.services.broadcast.domain.model.PhoneNumberCapacity;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Capacity as every instance sees it.
 *
 * <p>Written by the capacity listener when the Messaging Service publishes a change, and by the
 * degrader when Meta rate-limits a number. Read by the token bucket script on every acquisition,
 * which is why reads here are also cached briefly in {@link CapacityMemory} — the dispatch loop asks
 * far more often than the value changes.
 *
 * <p>Redis holds this rather than the database because it is on the hot path and because it must be
 * shared. The durable record stays in the Messaging Service's {@code waba_throughput_state}; this is
 * the fast copy, and losing it costs a fallback period rather than the value itself.
 */
@Component
public class RedisCapacityStore implements CapacityStorePort {

    private static final Logger log = LoggerFactory.getLogger(RedisCapacityStore.class);

    private static final Duration CAPACITY_TTL = Duration.ofHours(24);

    private static final String FIELD_CONFIGURED = "configuredMps";
    private static final String FIELD_EFFECTIVE = "effectiveMps";
    private static final String FIELD_TIER = "tier";
    private static final String FIELD_BACKOFF = "backoffUntilMs";
    private static final String FIELD_UPDATED = "updatedAtMs";
    private static final String FIELD_SOURCE = "source";

    private final StringRedisTemplate redis;
    private final CapacityMemory memory;
    private final BroadcastProperties properties;

    public RedisCapacityStore(
            StringRedisTemplate redis, CapacityMemory memory, BroadcastProperties properties) {
        this.redis = redis;
        this.memory = memory;
        this.properties = properties;
    }

    @Override
    public Optional<PhoneNumberCapacity> find(String phoneNumberId) {
        try {
            Map<Object, Object> hash = redis.opsForHash().entries(RedisKeys.capacity(phoneNumberId));
            if (hash == null || hash.isEmpty()) {
                return Optional.empty();
            }
            PhoneNumberCapacity capacity = new PhoneNumberCapacity(
                    phoneNumberId,
                    intValue(hash.get(FIELD_CONFIGURED), properties.rateLimit().defaultMps()),
                    intValue(hash.get(FIELD_EFFECTIVE), properties.rateLimit().defaultMps()),
                    stringValue(hash.get(FIELD_TIER)),
                    longValue(hash.get(FIELD_BACKOFF)),
                    longValue(hash.get(FIELD_UPDATED)),
                    sourceValue(hash.get(FIELD_SOURCE)));

            memory.remember(capacity);
            return Optional.of(capacity);

        } catch (DataAccessException e) {
            // The caller decides what to do without capacity; returning empty rather than throwing
            // keeps the dispatch loop free of Redis-specific error handling.
            log.warn("Could not read capacity phoneNumberId={} reason={}", phoneNumberId, e.toString());
            return memory.lastKnown(phoneNumberId)
                    .map(known -> known.withSource(CapacitySource.LOCAL_FALLBACK));
        }
    }

    @Override
    public void put(PhoneNumberCapacity capacity) {
        memory.remember(capacity);
        try {
            String key = RedisKeys.capacity(capacity.phoneNumberId());
            redis.opsForHash().putAll(key, Map.of(
                    FIELD_CONFIGURED, String.valueOf(capacity.configuredMps()),
                    FIELD_EFFECTIVE, String.valueOf(capacity.effectiveMps()),
                    FIELD_TIER, capacity.tier() == null ? "" : capacity.tier(),
                    FIELD_BACKOFF, String.valueOf(capacity.backoffUntilMs()),
                    FIELD_UPDATED, String.valueOf(capacity.updatedAtMs()),
                    FIELD_SOURCE, capacity.source().name()));
            redis.expire(key, CAPACITY_TTL);

            log.info("Capacity applied phoneNumberId={} effectiveMps={} configuredMps={} tier={} source={}",
                    capacity.phoneNumberId(), capacity.effectiveMps(), capacity.configuredMps(),
                    capacity.tier(), capacity.source());

        } catch (DataAccessException e) {
            log.error("Could not write capacity phoneNumberId={} reason={}",
                    capacity.phoneNumberId(), e.toString());
        }
    }

    @Override
    public void degrade(String phoneNumberId, int newEffectiveMps, long backoffUntilMs) {
        try {
            String key = RedisKeys.capacity(phoneNumberId);
            redis.opsForHash().putAll(key, Map.of(
                    FIELD_EFFECTIVE, String.valueOf(Math.max(1, newEffectiveMps)),
                    FIELD_BACKOFF, String.valueOf(backoffUntilMs),
                    FIELD_UPDATED, String.valueOf(System.currentTimeMillis()),
                    FIELD_SOURCE, CapacitySource.DEGRADED.name()));
            redis.expire(key, CAPACITY_TTL);

        } catch (DataAccessException e) {
            log.error("Could not degrade capacity phoneNumberId={} reason={}", phoneNumberId, e.toString());
        }
    }

    @Override
    public boolean tryAcquireDegradeLock(String phoneNumberId) {
        try {
            Boolean acquired = redis.opsForValue().setIfAbsent(
                    RedisKeys.degradeLock(phoneNumberId),
                    "1",
                    properties.rateLimit().degradeLockTtl());
            return Boolean.TRUE.equals(acquired);

        } catch (DataAccessException e) {
            // Without the lock we cannot tell one rate limit from four hundred, so decline to
            // degrade rather than risk halving the rate repeatedly down to nothing.
            log.warn("Could not acquire degrade lock phoneNumberId={} reason={}", phoneNumberId, e.toString());
            return false;
        }
    }

    private static int intValue(Object raw, int fallback) {
        try {
            return raw == null ? fallback : Integer.parseInt(raw.toString());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static long longValue(Object raw) {
        try {
            return raw == null ? 0L : Long.parseLong(raw.toString());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static String stringValue(Object raw) {
        return raw == null ? null : raw.toString();
    }

    private static CapacitySource sourceValue(Object raw) {
        if (raw == null) {
            return CapacitySource.CAPACITY_EVENT;
        }
        try {
            return CapacitySource.valueOf(raw.toString());
        } catch (IllegalArgumentException e) {
            return CapacitySource.CAPACITY_EVENT;
        }
    }
}
