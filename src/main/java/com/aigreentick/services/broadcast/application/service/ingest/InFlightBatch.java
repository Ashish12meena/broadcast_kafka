package com.aigreentick.services.broadcast.application.service.ingest;

import com.aigreentick.services.broadcast.domain.model.DispatchBatch;
import com.aigreentick.services.broadcast.domain.model.Recipient;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A batch being worked on, with the bookkeeping needed to know when it is finished.
 *
 * <p>The Kafka offset for a batch may only be acknowledged once every recipient in it has a
 * <em>durable</em> outcome. Kafka has no partial acknowledgement, so the offset represents the whole
 * message: commit it early and a crash loses the recipients that had not been sent yet, with no
 * record they existed.
 *
 * <p>{@code remaining} counts down as outcomes arrive, from any thread. {@code completed} guards the
 * completion callback so it runs exactly once even if the count reaches zero twice under a race.
 *
 * <p>Used as a map key by {@code ResultCollector}, which is why identity equality is left alone —
 * two batches with identical contents are still two separate units of work with two separate
 * offsets.
 */
public final class InFlightBatch {

    private final DispatchBatch batch;
    private final Runnable onComplete;
    private final AtomicInteger remaining;
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final long acceptedAtMs = System.currentTimeMillis();

    /** Index of the next recipient to dispatch. Only ever touched under the queue's lock. */
    private int cursor;

    public InFlightBatch(DispatchBatch batch, Runnable onComplete) {
        this.batch = batch;
        this.onComplete = onComplete;
        this.remaining = new AtomicInteger(batch.size());
    }

    public DispatchBatch batch() {
        return batch;
    }

    public Long campaignId() {
        return batch.campaignId();
    }

    public String phoneNumberId() {
        return batch.phoneNumberId();
    }

    /** The campaign run this batch belongs to, echoed onto every outcome it produces. */
    public String traceId() {
        return batch.traceId();
    }

    public long ageMs() {
        return System.currentTimeMillis() - acceptedAtMs;
    }

    boolean hasMoreToDispatch() {
        return cursor < batch.size();
    }

    Recipient nextRecipient() {
        return batch.recipients().get(cursor++);
    }

    int undispatched() {
        return batch.size() - cursor;
    }

    /**
     * Records that one recipient is resolved.
     *
     * @return true when this was the last one, meaning the caller should complete the batch
     */
    public boolean recordResolved() {
        return remaining.decrementAndGet() <= 0;
    }

    /** Runs the completion callback at most once. */
    public void complete() {
        if (completed.compareAndSet(false, true)) {
            onComplete.run();
        }
    }

    public boolean isCompleted() {
        return completed.get();
    }
}
