package com.aigreentick.services.broadcast.domain.model;

/**
 * What one phone number is currently allowed to do.
 *
 * <p>The two rates are deliberately separate. {@code configuredMps} is the ceiling Meta grants the
 * number via its throughput tier — not ours to choose. {@code effectiveMps} is the rate actually in
 * use, reduced after a rate-limit response and recovered gradually afterwards by the Messaging
 * Service. {@code effectiveMps} below {@code configuredMps} is the normal state after a burst of
 * 429s, not a fault.
 */
public record PhoneNumberCapacity(
        String phoneNumberId,
        int configuredMps,
        int effectiveMps,
        String tier,
        long backoffUntilMs,
        long updatedAtMs,
        CapacitySource source) {

    public boolean inBackoff(long nowMs) {
        return backoffUntilMs > nowMs;
    }

    public PhoneNumberCapacity withSource(CapacitySource newSource) {
        return new PhoneNumberCapacity(
                phoneNumberId, configuredMps, effectiveMps, tier, backoffUntilMs, updatedAtMs, newSource);
    }
}
