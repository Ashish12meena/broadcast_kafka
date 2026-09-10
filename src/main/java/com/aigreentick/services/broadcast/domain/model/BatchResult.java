package com.aigreentick.services.broadcast.domain.model;

import java.util.List;

/**
 * A group of outcomes for one campaign and phone number, published as a single event.
 *
 * <h2>Still a group, but now a small one</h2>
 * The grouping used to be done by {@code ResultCollector}'s in-heap buffer, which accumulated a
 * few hundred outcomes before publishing. That buffer was the only copy of results for messages the
 * customer had already received, so losing a pod lost them.
 *
 * <p>Grouping now happens in the Kafka producer, via {@code linger.ms} and {@code batch.size} —
 * same wire efficiency, durable the moment the broker acknowledges. This record usually carries one
 * outcome; the list is kept because the Messaging Service's callback applies a batch in one
 * transaction and there is no reason to give that up if a future path has several in hand.
 *
 * @param traceId the campaign run, echoed from the dispatch event
 */
public record BatchResult(
        Long campaignId,
        String phoneNumberId,
        String traceId,
        List<RecipientOutcome> outcomes) {
}
