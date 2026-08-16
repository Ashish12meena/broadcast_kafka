package com.aigreentick.services.broadcast.application.service.ingest;

import com.aigreentick.services.broadcast.domain.model.Recipient;

/**
 * One recipient about to be sent, carrying the batch it belongs to and how many attempts it has had.
 *
 * <p>The attempt count travels with the work rather than living in a side map, so a retry re-entering
 * the queue keeps its history and cannot loop indefinitely.
 */
public final class PendingSend {

    private final InFlightBatch batch;
    private final Recipient recipient;
    private int attempts;

    public PendingSend(InFlightBatch batch, Recipient recipient) {
        this.batch = batch;
        this.recipient = recipient;
    }

    public InFlightBatch batch() {
        return batch;
    }

    public Recipient recipient() {
        return recipient;
    }

    public String phoneNumberId() {
        return batch.phoneNumberId();
    }

    public int attempts() {
        return attempts;
    }

    public int recordAttempt() {
        return ++attempts;
    }
}
