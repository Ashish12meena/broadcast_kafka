package com.aigreentick.services.broadcast.application.service.dispatch;

import com.aigreentick.services.broadcast.application.service.ingest.InFlightBatch;
import com.aigreentick.services.broadcast.application.service.ingest.PendingSend;
import com.aigreentick.services.broadcast.application.service.ingest.PhoneNumberQueue;
import com.aigreentick.services.broadcast.infrastructure.observability.BroadcastMetrics;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns one queue per active phone number and exactly one worker draining each.
 *
 * <p>
 * One worker per number keeps the pacing decision in a single place per number,
 * so the token
 * bucket is asked by one caller at a time rather than by every send thread
 * independently. Different
 * numbers never wait on each other.
 *
 * <p>
 * Workers are virtual threads. A worker spends most of its life asleep waiting
 * for tokens, and a
 * platform thread per phone number would put a hard ceiling on how many numbers
 * one instance can
 * serve for no reason other than thread cost.
 */
@Service
public class DispatchScheduler {

    private static final Logger log = LoggerFactory.getLogger(DispatchScheduler.class);

    private final Map<String, PhoneNumberQueue> queues = new ConcurrentHashMap<>();
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    private final ExecutorService dispatchExecutor;
    private final DispatchWorkerFactory workerFactory;
    private final BroadcastMetrics metrics;

    public DispatchScheduler(
            @Qualifier("dispatchExecutor") ExecutorService dispatchExecutor,
            DispatchWorkerFactory workerFactory,
            BroadcastMetrics metrics) {
        this.dispatchExecutor = dispatchExecutor;
        this.workerFactory = workerFactory;
        this.metrics = metrics;
    }

    public void enqueue(InFlightBatch batch) {
        PhoneNumberQueue queue = queueFor(batch.phoneNumberId());
        queue.offer(batch);
        startWorkerIfIdle(queue);
    }

    /**
     * Returns an attempted send to its queue and makes sure something is draining
     * it.
     */
    public void requeue(PendingSend send) {
        PhoneNumberQueue queue = queueFor(send.phoneNumberId());
        queue.requeue(send);
        startWorkerIfIdle(queue);
    }

    /**
     * Restarts draining of a queue that already exists.
     *
     * <p>
     * Called by a worker that is standing down and finds work has arrived in the
     * meantime. Nobody
     * else would notice: the producers only start a worker when they add to an idle
     * queue, and from
     * their point of view this queue already had one.
     */
    public void enqueueExistingQueue(PhoneNumberQueue queue) {
        startWorkerIfIdle(queue);
    }

    private PhoneNumberQueue queueFor(String phoneNumberId) {
        return queues.computeIfAbsent(phoneNumberId, PhoneNumberQueue::new);
    }

    private void startWorkerIfIdle(PhoneNumberQueue queue) {
        if (shuttingDown.get()) {
            return;
        }
        if (queue.tryStartWorker()) {
            dispatchExecutor.submit(workerFactory.create(queue));
        }
    }

    /**
     * The deepest per-number backlog, which is what the backpressure decision is
     * made on.
     */
    public int deepestQueue() {
        int deepest = 0;
        for (PhoneNumberQueue queue : queues.values()) {
            deepest = Math.max(deepest, queue.queuedBatches());
        }
        return deepest;
    }

    public int totalPendingRecipients() {
        int total = 0;
        for (PhoneNumberQueue queue : queues.values()) {
            total += queue.pendingRecipients();
        }
        return total;
    }

    public int activeNumbers() {
        return (int) queues.values().stream().filter(queue -> !queue.isEmpty()).count();
    }

    public Map<String, Integer> depthByPhoneNumber() {
        Map<String, Integer> depths = new ConcurrentHashMap<>();
        queues.forEach((phoneNumberId, queue) -> {
            int pending = queue.pendingRecipients();
            if (pending > 0) {
                depths.put(phoneNumberId, pending);
            }
        });
        return depths;
    }

    public boolean isShuttingDown() {
        return shuttingDown.get();
    }

    /** Publishes queue depth and drops queues for numbers that have gone quiet. */
    @Scheduled(fixedDelayString = "${broadcast.dispatch.housekeeping-interval:PT60S}")
    public void housekeeping() {
        metrics.queueState(totalPendingRecipients(), activeNumbers());

        queues.entrySet().removeIf(entry -> {
            PhoneNumberQueue queue = entry.getValue();
            return queue.isEmpty() && !queue.isWorkerActive();
        });
    }

    /**
     * Stops accepting work and lets the workers finish what they hold.
     *
     * <p>
     * Only the flag is set here. The executor is closed by its own bean lifecycle
     * after this
     * runs, which matters: shutting down the executor from inside a component whose
     * workers are
     * running on that same executor is how in-flight sends get rejected and lost.
     */
    @PreDestroy
    public void shutdown() {
        shuttingDown.set(true);
        log.info("Dispatch scheduler shutting down; pendingRecipients={} activeNumbers={}",
                totalPendingRecipients(), activeNumbers());
    }
}
