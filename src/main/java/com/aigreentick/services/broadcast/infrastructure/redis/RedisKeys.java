package com.aigreentick.services.broadcast.infrastructure.redis;

import com.aigreentick.services.broadcast.common.constants.InfraConstants;


/**
 * Builds every Redis key this service uses, from the prefixes in {@link InfraConstants.Redis}.
 *
 * <p>The braces around the phone number are a Redis Cluster hash tag, not decoration. The token
 * bucket script reads the capacity hash and writes the bucket hash in one call, and Redis Cluster
 * rejects a script whose keys live on different slots — the shared tag guarantees they do not.
 */
public final class RedisKeys {

    private RedisKeys() {
    }

    /** Capacity for one number: effective and configured rate, tier, backoff. */
    public static String capacity(String phoneNumberId) {
        return InfraConstants.Redis.CAPACITY_KEY_PREFIX + hashTagged(phoneNumberId);
    }

    /** Token bucket state for one number. */
    public static String tokenBucket(String phoneNumberId) {
        return InfraConstants.Redis.TOKEN_BUCKET_KEY_PREFIX + hashTagged(phoneNumberId);
    }

    /** Short-lived lock collapsing a burst of rate-limit responses into one degrade. */
    public static String degradeLock(String phoneNumberId) {
        return InfraConstants.Redis.DEGRADE_LOCK_KEY_PREFIX + hashTagged(phoneNumberId);
    }

    /** Duplicate-send guard for one recipient. */
    public static String sentClaim(Long recipientId) {
        return InfraConstants.Redis.SENT_CLAIM_KEY_PREFIX + recipientId;
    }

    /**
     * Wraps the phone number in a Redis Cluster hash tag so every key for one number lands on the
     * same slot, which is what lets the token bucket script touch two of them in one call.
     */
    private static String hashTagged(String phoneNumberId) {
        return InfraConstants.Redis.HASH_TAG_OPEN + phoneNumberId + InfraConstants.Redis.HASH_TAG_CLOSE;
    }
}
