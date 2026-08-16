package com.aigreentick.services.broadcast.application.service.capacity;

import com.aigreentick.services.broadcast.application.port.out.CapacityStorePort;
import com.aigreentick.services.broadcast.domain.model.PhoneNumberCapacity;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import com.aigreentick.services.broadcast.infrastructure.observability.BroadcastMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Reduces a phone number's rate when Meta says it is sending too fast.
 *
 * <h2>Halve immediately, recover elsewhere</h2>
 * This service only ever reduces. Recovery back toward the ceiling is the Messaging Service's job,
 * on a timer, because recovery is a function of elapsed time rather than of anything a sender
 * observes. Fast down and slow up is the standard shape for a control loop that must not oscillate:
 * an eager recovery walks straight back into the limit it just escaped.
 *
 * <h2>One degrade per burst</h2>
 * Four hundred concurrent sends hitting the same limit describe one condition, not four hundred. A
 * short lock in Redis collapses them, because halving on each would take a thousand-per-second
 * number down to one within a second and leave it there.
 *
 * <h2>Redis now, database eventually</h2>
 * The reduction is written to Redis so it takes effect within milliseconds for every instance. The
 * durable record is made by the Messaging Service when it receives the outcome carrying the
 * rate-limit code. Redis is the fast path; the database is the one that survives a restart.
 */
@Service
public class CapacityDegrader {

    private static final Logger log = LoggerFactory.getLogger(CapacityDegrader.class);

    private final CapacityStorePort capacityStore;
    private final CapacityService capacityService;
    private final BroadcastProperties properties;
    private final BroadcastMetrics metrics;

    public CapacityDegrader(
            CapacityStorePort capacityStore,
            CapacityService capacityService,
            BroadcastProperties properties,
            BroadcastMetrics metrics) {
        this.capacityStore = capacityStore;
        this.capacityService = capacityService;
        this.properties = properties;
        this.metrics = metrics;
    }

    public void degradeAfterRateLimit(String phoneNumberId) {
        if (!capacityStore.tryAcquireDegradeLock(phoneNumberId)) {
            log.debug("Rate limit for phoneNumberId={} folded into an in-progress degrade", phoneNumberId);
            return;
        }

        PhoneNumberCapacity current = capacityStore.find(phoneNumberId)
                .orElseGet(() -> capacityService.defaultCapacity(phoneNumberId));

        int reduced = Math.max(1, current.effectiveMps() / 2);
        long backoffUntil = System.currentTimeMillis() + properties.rateLimit().degradeLockTtl().toMillis();

        capacityStore.degrade(phoneNumberId, reduced, backoffUntil);
        metrics.degraded(phoneNumberId);

        log.warn("Throughput reduced phoneNumberId={} {} -> {} mps after a Meta rate limit",
                phoneNumberId, current.effectiveMps(), reduced);
    }

    /**
     * Suppresses a number that is mid throughput upgrade.
     *
     * <p>Meta documents the upgrade as taking up to about a minute, during which the number is
     * unusable. The rate is left alone deliberately — this number is being made faster, and halving
     * it would punish exactly the wrong event.
     */
    public void suppressForUpgrade(String phoneNumberId) {
        long backoffUntil = System.currentTimeMillis() + properties.rateLimit().upgradeBackoff().toMillis();

        capacityStore.find(phoneNumberId).ifPresentOrElse(
                current -> capacityStore.degrade(phoneNumberId, current.effectiveMps(), backoffUntil),
                () -> capacityStore.degrade(
                        phoneNumberId, properties.rateLimit().defaultMps(), backoffUntil));

        log.info("Phone number {} is being upgraded by Meta; suppressed until {}",
                phoneNumberId, backoffUntil);
    }
}
