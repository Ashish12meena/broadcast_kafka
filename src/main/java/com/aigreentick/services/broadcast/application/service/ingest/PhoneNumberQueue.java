package com.aigreentick.services.broadcast.application.service.ingest;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Pending work for one phone number, organised so that no campaign can monopolise it.
 *
 * <h2>Why not one queue per number</h2>
 * A single first-in-first-out queue serves campaigns strictly in arrival order. A hundred-thousand
 * recipient campaign whose batches arrive first drains completely before a five-hundred recipient
 * campaign on the same number sends anything at all. Splitting by campaign and rotating between them
 * costs one map and one rotation list, and removes that failure entirely.
 *
 * <h2>Retries are served first</h2>
 * Work that has already been attempted goes to the front. It is older than anything in the batches,
 * it is usually a rate limit that has since cleared, and leaving it behind a large campaign is how a
 * transient failure turns into a stale one.
 *
 * <p>Methods that touch the campaign map are synchronized. Contention is low: one dispatch worker
 * drains a number while the Kafka consumer threads occasionally add to it.
 */
public final class PhoneNumberQueue {

    private final String phoneNumberId;

    /** Campaign identifier to its outstanding batches. Insertion-ordered for a stable rotation. */
    private final Map<Long, Deque<InFlightBatch>> campaigns = new LinkedHashMap<>();

    /** Round-robin cursor over the campaign identifiers. */
    private final Deque<Long> rotation = new ArrayDeque<>();

    /** Attempted sends waiting to go again. Lock-free: retries arrive from send threads. */
    private final ConcurrentLinkedQueue<PendingSend> retries = new ConcurrentLinkedQueue<>();

    private final AtomicBoolean workerActive = new AtomicBoolean(false);

    private int queuedBatches;
    private int pendingRecipients;

    public PhoneNumberQueue(String phoneNumberId) {
        this.phoneNumberId = phoneNumberId;
    }

    public String phoneNumberId() {
        return phoneNumberId;
    }

    public synchronized void offer(InFlightBatch batch) {
        campaigns.computeIfAbsent(batch.campaignId(), id -> {
            rotation.addLast(id);
            return new ArrayDeque<>();
        }).addLast(batch);

        queuedBatches++;
        pendingRecipients += batch.batch().size();
    }

    /** Puts an attempted send back in line, ahead of untried work. */
    public void requeue(PendingSend send) {
        retries.add(send);
    }

    /**
     * Takes up to {@code max} sends, rotating across campaigns so each gets a turn.
     *
     * <p>The rotation advances one campaign per recipient rather than per batch. Advancing per batch
     * would let a campaign with thousand-recipient batches take a thousand slots for every one taken
     * by a campaign with small batches.
     */
    public List<PendingSend> drain(int max) {
        List<PendingSend> drained = new ArrayList<>(Math.min(max, 128));

        // Retries first, and outside the lock: they came from send threads and need no rotation.
        while (drained.size() < max) {
            PendingSend retry = retries.poll();
            if (retry == null) {
                break;
            }
            drained.add(retry);
        }

        synchronized (this) {
            while (drained.size() < max && !rotation.isEmpty()) {
                Long campaignId = rotation.pollFirst();
                Deque<InFlightBatch> batches = campaigns.get(campaignId);

                if (batches == null || batches.isEmpty()) {
                    campaigns.remove(campaignId);
                    continue;
                }

                InFlightBatch head = batches.peekFirst();
                if (!head.hasMoreToDispatch()) {
                    // Fully handed out. It stays alive through its own remaining counter until every
                    // outcome is recorded, but this queue is done with it.
                    batches.pollFirst();
                    queuedBatches--;
                    rotation.addLast(campaignId);
                    continue;
                }

                drained.add(new PendingSend(head, head.nextRecipient()));
                pendingRecipients--;

                // Back of the rotation: this campaign has had its turn.
                rotation.addLast(campaignId);
            }
        }
        return drained;
    }

    public synchronized int pendingRecipients() {
        return pendingRecipients + retries.size();
    }

    public synchronized int queuedBatches() {
        return queuedBatches;
    }

    public synchronized boolean isEmpty() {
        return pendingRecipients <= 0 && retries.isEmpty();
    }

    public boolean tryStartWorker() {
        return workerActive.compareAndSet(false, true);
    }

    public boolean tryStopWorker() {
        return workerActive.compareAndSet(true, false);
    }

    public void forceStopWorker() {
        workerActive.set(false);
    }

    public boolean isWorkerActive() {
        return workerActive.get();
    }
}
