package com.aigreentick.services.broadcast.infrastructure.redis;

import com.aigreentick.services.broadcast.application.port.out.RateLimiterPort;
import com.aigreentick.services.broadcast.domain.model.CapacitySource;
import com.aigreentick.services.broadcast.domain.model.PhoneNumberCapacity;
import com.aigreentick.services.broadcast.domain.model.RateGrant;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The in-process bucket used when Redis cannot answer.
 *
 * <p>Deliberately pessimistic. Its rate is a fraction of the last known capacity divided by an
 * assumed instance count, so N instances all falling back together still land under the real limit.
 * Over-estimating the instance count is the safe direction.
 *
 * <p>The point is to keep sending slowly rather than either stopping entirely or sending at full
 * rate with no coordination. Both alternatives are worse: stopping turns a cache outage into a
 * platform outage, and continuing at full rate multiplies the limit by the replica count.
 *
 * <p>Time in this bucket comes from {@link System#nanoTime()} rather than wall-clock, so an NTP
 * correction cannot hand out a windfall of tokens or freeze the bucket.
 */
@Component
public class LocalFallbackRateLimiter implements RateLimiterPort {

    private final BroadcastProperties properties;
    private final CapacityMemory capacityMemory;
    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

    public LocalFallbackRateLimiter(BroadcastProperties properties, CapacityMemory capacityMemory) {
        this.properties = properties;
        this.capacityMemory = capacityMemory;
    }

    @Override
    public RateGrant acquire(String phoneNumberId, int requested) {
        int mps = fallbackRateFor(phoneNumberId);
        Bucket bucket = buckets.computeIfAbsent(phoneNumberId, key -> new Bucket());
        return bucket.take(requested, mps, properties.rateLimit().burstSeconds());
    }

    private int fallbackRateFor(String phoneNumberId) {
        BroadcastProperties.RateLimit settings = properties.rateLimit();

        int lastKnown = capacityMemory.lastKnown(phoneNumberId)
                .map(PhoneNumberCapacity::effectiveMps)
                .orElse(settings.defaultMps());

        int share = (int) Math.floor(lastKnown * settings.fallbackFraction() / settings.assumedInstances());
        return Math.max(1, share);
    }

    /** Last-known capacity, remembered so a fallback is informed rather than blind. */
    public CapacitySource sourceFor(String phoneNumberId) {
        return capacityMemory.lastKnown(phoneNumberId).isPresent()
                ? CapacitySource.LOCAL_FALLBACK
                : CapacitySource.DEFAULT;
    }

    private static final class Bucket {

        private double tokens = -1;
        private long lastRefillNanos;

        synchronized RateGrant take(int requested, int mps, double burstSeconds) {
            long now = System.nanoTime();
            double burst = mps * burstSeconds;

            if (tokens < 0) {
                tokens = burst;
                lastRefillNanos = now;
            }

            double elapsedSeconds = Math.max(0, now - lastRefillNanos) / 1_000_000_000d;
            tokens = Math.min(burst, tokens + elapsedSeconds * mps);
            lastRefillNanos = now;

            int granted = (int) Math.floor(Math.min(tokens, requested));
            if (granted > 0) {
                tokens -= granted;
                return RateGrant.of(granted, 0);
            }

            long waitMicros = (long) Math.ceil((1 - tokens) * 1_000_000 / mps);
            return RateGrant.none(Math.max(0, waitMicros));
        }
    }
}
