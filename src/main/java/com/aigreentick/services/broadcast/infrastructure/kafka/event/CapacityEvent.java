package com.aigreentick.services.broadcast.infrastructure.kafka.event;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A phone number's capacity, published by the Messaging Service whenever it changes.
 *
 * <p>The topic is log-compacted and keyed on {@code phoneNumberId}, so a restarting instance replays
 * the current value for every number it has ever heard about. No bulk API call at startup, no cold
 * cache, and no dependency on the Messaging Service being reachable at the moment this one boots.
 *
 * @param configuredMps the ceiling from Meta's throughput tier — not ours to choose
 * @param effectiveMps  the working rate, which sits below the ceiling after a rate limit and climbs
 *                      back gradually. Below the ceiling is the normal state after a burst of 429s
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CapacityEvent(
        @JsonAlias("phone_number_id") String phoneNumberId,
        @JsonAlias("waba_phone_number_id") Long wabaPhoneNumberId,
        @JsonAlias("organization_id") Long organizationId,
        @JsonAlias("configured_mps") Integer configuredMps,
        @JsonAlias("effective_mps") Integer effectiveMps,
        String tier,
        @JsonAlias("backoff_until_ms") Long backoffUntilMs,
        @JsonAlias("updated_at_ms") Long updatedAtMs) {

    public boolean isUsable() {
        return phoneNumberId != null && !phoneNumberId.isBlank() && effectiveMps != null && effectiveMps > 0;
    }
}
