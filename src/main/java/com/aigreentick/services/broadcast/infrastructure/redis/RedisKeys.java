package com.aigreentick.services.broadcast.infrastructure.redis;

/**
 * Every Redis key this service uses, in one place.
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
        return "wa:cap:{" + phoneNumberId + "}";
    }

    /** Token bucket state for one number. */
    public static String tokenBucket(String phoneNumberId) {
        return "wa:tb:{" + phoneNumberId + "}";
    }

    /** Short-lived lock collapsing a burst of rate-limit responses into one degrade. */
    public static String degradeLock(String phoneNumberId) {
        return "wa:degradelock:{" + phoneNumberId + "}";
    }

    /** Duplicate-send guard for one recipient. */
    public static String sentClaim(Long recipientId) {
        return "wa:sent:" + recipientId;
    }
}
