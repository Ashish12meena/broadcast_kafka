package com.aigreentick.services.broadcast.domain.model;

import java.util.List;

/**
 * One unit of work as it arrives from the Messaging Service: a set of recipients that all send from
 * the same phone number on behalf of the same campaign.
 *
 * <p>{@code phoneNumberId} is Meta's identifier and is the key for every rate decision in this
 * service. {@code wabaAccountId} is the platform's own identifier, carried only for logging and
 * reconciliation — the two are different values and must never be substituted for one another.
 *
 * <p>{@code accessToken} arrives on the Kafka payload. See {@code DispatchEvent} for why that is a
 * known problem and what moving the fetch to this service would actually require.
 *
 * @param traceId the campaign run, carried through to the result event so one recipient can be
 *                followed end to end across both services
 */
public record DispatchBatch(
        Long campaignId,
        String phoneNumberId,
        Long wabaAccountId,
        String accessToken,
        String traceId,
        List<Recipient> recipients) {

    public int size() {
        return recipients == null ? 0 : recipients.size();
    }
}
