package com.aigreentick.services.broadcast.domain.model;

import java.util.List;

/**
 * A group of outcomes for one campaign and phone number, published as a single event.
 *
 * <p>Grouped rather than sent per recipient so the Messaging Service can apply them in one
 * transaction and one bulk update, as its callback service already does.
 */
public record BatchResult(
        Long campaignId,
        String phoneNumberId,
        List<RecipientOutcome> outcomes) {
}
