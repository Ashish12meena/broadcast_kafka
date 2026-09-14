package com.aigreentick.services.broadcast.application.service.dispatch;

import com.aigreentick.services.broadcast.common.constants.InfraConstants;
import com.aigreentick.services.broadcast.application.service.ingest.InFlightBatch;
import com.aigreentick.services.broadcast.application.service.ingest.PendingSend;
import com.aigreentick.services.broadcast.application.service.ingest.PhoneNumberQueue;
import com.aigreentick.services.broadcast.infrastructure.observability.BroadcastMetrics;
import com.aigreentick.services.broadcast.infrastructure.redis.RedisQueueDepthPublisher;
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
    private final RedisQueueDepthPublisher depthPublisher;

    public DispatchScheduler(
            @Qualifier(InfraConstants.Executor.DISPATCH_EXECUTOR) ExecutorService dispatchExecutor,
            DispatchWorkerFactory workerFactory,
            BroadcastMetrics metrics,
            RedisQueueDepthPublisher depthPublisher) {
        this.dispatchExecutor = dispatchExecutor;
        this.workerFactory = workerFactory;
        this.metrics = metrics;
        this.depthPublisher = depthPublisher;
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

    /**
     * Depths for numbers with work outstanding, for the ops endpoint.
     *
     * <p>Omits the zeros deliberately: an operator asking what is in flight does not want a row per
     * number that has nothing. Use {@link #allDepthsByPhoneNumber()} for anything that feeds the
     * upstream credit loop, where the zeros carry meaning.
     */
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

    /**
     * Every known number's depth, zeros included.
     *
     * <p>Separate from {@link #depthByPhoneNumber()} because the zeros are the point here. A number
     * whose key is absent reads upstream as "I cannot see the queue", which correctly produces a
     * conservative fallback claim. A number whose key says zero reads as "the queue is empty, send
     * what you have". Those are opposite instructions, and publishing only the non-zero depths
     * would silently turn every idle number into the first case.
     */
    public Map<String, Integer> allDepthsByPhoneNumber() {
        Map<String, Integer> depths = new ConcurrentHashMap<>();
        queues.forEach((phoneNumberId, queue) -> depths.put(phoneNumberId, queue.pendingRecipients()));
        return depths;
    }

    public boolean isShuttingDown() {
        return shuttingDown.get();
    }

    /**
     * Publishes queue depth and drops queues for numbers that have gone quiet.
     *
     * <p>This javadoc used to be wrong. The method recorded a Micrometer gauge and evicted queues;
     * it never touched the Redis depth key, so the only writer was {@code DispatchWorker}, which
     * stops writing the instant it exits. An idle number's reading therefore expired and stayed
     * expired, and the Messaging Service — polling a few seconds later — read nothing and fell back
     * on every claim. Publishing here is what makes a reading survive a worker standing down.
     */
    @Scheduled(fixedDelayString = InfraConstants.ConfigKeys.DISPATCH_HOUSEKEEPING_INTERVAL)
    public void housekeeping() {
        metrics.queueState(totalPendingRecipients(), activeNumbers());
        refreshQueueDepths();

        // Eviction runs after the refresh, never before: a queue dropped on this pass has already
        // been published as zero, so the reader sees "empty" rather than the key's last non-zero
        // value sitting there until the TTL clears it.
        queues.entrySet().removeIf(entry -> {
            PhoneNumberQueue queue = entry.getValue();
            return queue.isEmpty() && !queue.isWorkerActive();
        });
    }

    /**
     * Rewrites every known number's depth key.
     *
     * <p>Failure is swallowed rather than propagated. Redis being unreachable costs the upstream
     * credit loop a conservative claim; letting it abort this method would also skip the eviction
     * sweep below and leak a queue object per number that has gone quiet.
     */
    private void refreshQueueDepths() {
        try {
            allDepthsByPhoneNumber().forEach(depthPublisher::publish);
        } catch (RuntimeException e) {
            log.warn("Queue depth refresh failed; readings will age out until the next pass", e);
        }
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