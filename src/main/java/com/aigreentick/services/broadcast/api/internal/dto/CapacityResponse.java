package com.aigreentick.services.broadcast.api.internal.dto;

/**
 * What this instance believes a phone number may currently do.
 *
 * <p>{@code source} is the field to read first during an incident: it distinguishes a rate published
 * by the Messaging Service from one this service reduced itself, and from a local guess made because
 * Redis was unreachable.
 */
public record CapacityResponse(
        String phoneNumberId,
        int configuredMps,
        int effectiveMps,
        String tier,
        long backoffUntilMs,
        long updatedAtMs,
        String source) {
}
