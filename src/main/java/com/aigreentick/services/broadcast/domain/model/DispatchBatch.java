package com.aigreentick.services.broadcast.domain.model;

import java.util.List;

/**
 * One unit of work as it arrives from the Messaging Service: a set of recipients that all send from
 * the same phone number on behalf of the same campaign.
 *
 * <p>{@code phoneNumberId} is Meta's identifier and is the key for every rate decision in this
 * service. {@code wabaAccountId} is the platform's own identifier, carried only for logging and
 * reconciliation — the two are different values and must never be substituted for one another.
 */
public record DispatchBatch(
        Long campaignId,
        String phoneNumberId,
        Long wabaAccountId,
        String accessToken,
        List<Recipient> recipients) {

    public int size() {
        return recipients == null ? 0 : recipients.size();
    }
}
