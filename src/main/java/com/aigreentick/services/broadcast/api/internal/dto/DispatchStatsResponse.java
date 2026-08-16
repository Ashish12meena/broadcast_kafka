package com.aigreentick.services.broadcast.api.internal.dto;

import java.util.Map;

/**
 * A snapshot of what this instance is doing right now.
 *
 * <p>For operators during an incident, when the question is "is this pod stuck or merely waiting?"
 * — a distinction that queue depth alone cannot answer but queue depth plus consumer state can.
 */
public record DispatchStatsResponse(
        int activePhoneNumbers,
        int pendingRecipients,
        boolean consumerPaused,
        boolean shuttingDown,
        Map<String, Integer> pendingByPhoneNumber) {
}
