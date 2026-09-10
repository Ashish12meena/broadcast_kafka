package com.aigreentick.services.broadcast.application.service.dispatch;

import com.aigreentick.services.broadcast.common.constants.DomainConstants;
import com.aigreentick.services.broadcast.application.port.out.RateLimiterPort;
import com.aigreentick.services.broadcast.application.service.ingest.ConsumerFlowController;
import com.aigreentick.services.broadcast.application.service.ingest.PendingSend;
import com.aigreentick.services.broadcast.application.service.ingest.PhoneNumberQueue;
import com.aigreentick.services.broadcast.domain.model.RateGrant;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import com.aigreentick.services.broadcast.infrastructure.observability.BroadcastMetrics;
import com.aigreentick.services.broadcast.infrastructure.redis.RedisQueueDepthPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * Drains one phone number's queue at the rate the shared meter allows.
 *
 * <h2>Continuous, not windowed</h2>
 * The loop asks for tokens, sends what it is granted, and asks again. It never waits for the sends
 * it submitted to finish. An earlier arrangement that dispatched a fixed window and blocked until
 * all of them returned achieved {@code windowSize / p99Latency} messages per second rather than
 * {@code windowSize} per second — the barrier was the bottleneck, not the limit.
 *
 * <h2>Asking for only what is wanted</h2>
 * The request is {@code min(chunkSize, pendingRecipients)}. Asking for a full chunk when ten
 * recipients remain would spend tokens that are then thrown away, and those tokens are the number's
 * real capacity.
 *
 * <h2>The depth is published every pass</h2>
 * This loop is the only place that knows, moment to moment, how much work is outstanding for a
 * number. Publishing it here — rather than on a timer — means the reading is never older than one
 * iteration, which is what lets the Messaging Service's credit loop be tight enough to be useful
 * without being so tight that it oscillates.
 */
public final class DispatchWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(DispatchWorker.class);

    private final PhoneNumberQueue queue;
    private final RateLimiterPort rateLimiter;
    private final SendExecutor sendExecutor;
    private final DispatchScheduler scheduler;
    private final ConsumerFlowController flowController;
    private final BroadcastProperties properties;
    private final BroadcastMetrics metrics;
    private final RedisQueueDepthPublisher depthPublisher;

    DispatchWorker(
            PhoneNumberQueue queue,
            RateLimiterPort rateLimiter,
            SendExecutor sendExecutor,
            DispatchScheduler scheduler,
            ConsumerFlowController flowController,
            BroadcastProperties properties,
            BroadcastMetrics metrics,
            RedisQueueDepthPublisher depthPublisher) {
        this.queue = queue;
        this.rateLimiter = rateLimiter;
        this.sendExecutor = sendExecutor;
        this.scheduler = scheduler;
        this.flowController = flowController;
        this.properties = properties;
        this.metrics = metrics;
        this.depthPublisher = depthPublisher;
    }

    @Override
    public void run() {
        String phoneNumberId = queue.phoneNumberId();
        log.debug("Dispatch worker started phoneNumberId={}", phoneNumberId);

        try {
            while (!scheduler.isShuttingDown()) {
                int pending = queue.pendingRecipients();

                // Published before the token request rather than after the drain. Publishing after
                // would report the depth the queue had once this pass's work was already handed
                // out, which is systematically low by one chunk — and a systematically low depth
                // is a standing invitation to the upstream credit loop to over-claim.
                depthPublisher.publish(phoneNumberId, pending);

                if (queue.isEmpty() && shouldExit()) {
                    // Zero on the way out, so a drained number's key does not sit at its last
                    // non-zero value until the TTL clears it. That gap would stall the upstream
                    // claim for a number that is in fact idle.
                    depthPublisher.publish(phoneNumberId, 0);
                    return;
                }

                int wanted = Math.min(properties.dispatch().chunkSize(), pending);
                if (wanted <= 0) {
                    continue;
                }

                RateGrant grant = rateLimiter.acquire(phoneNumberId, wanted);
                if (grant.isEmpty()) {
                    sleepFor(grant.waitMicros(), phoneNumberId);
                    continue;
                }

                List<PendingSend> sends = queue.drain(grant.granted());
                for (PendingSend send : sends) {
                    // Submitted and not awaited. The next token acquisition happens while these
                    // are still in flight, which is what keeps the rate continuous.
                    sendExecutor.submit(send);
                }

                relieveBackpressureIfDrained();
            }
        } catch (RuntimeException e) {
            log.error("Dispatch worker failed phoneNumberId={}", phoneNumberId, e);
        } finally {
            queue.forceStopWorker();
            // Work may have arrived between the last check and releasing the flag. Nothing else
            // will notice, so this worker restarts the queue itself.
            if (!queue.isEmpty() && !scheduler.isShuttingDown()) {
                scheduler.enqueueExistingQueue(queue);
            }
            log.debug("Dispatch worker exiting phoneNumberId={}", queue.phoneNumberId());
        }
    }

    /**
     * Releases the worker flag and re-checks, because a batch can arrive in the gap between finding
     * the queue empty and standing down.
     */
    private boolean shouldExit() {
        if (!queue.tryStopWorker()) {
            return true;
        }
        if (queue.isEmpty()) {
            return true;
        }
        return !queue.tryStartWorker();
    }

    private void sleepFor(long waitMicros, String phoneNumberId) {
        long millis = Math.max(DomainConstants.Dispatch.MIN_SLEEP_MILLIS,
                waitMicros / DomainConstants.Dispatch.MICROS_PER_MILLI);
        long capped = Math.min(millis, properties.dispatch().maxSleep().toMillis());
        metrics.rateLimitWait(phoneNumberId, Duration.ofMillis(capped));
        try {
            Thread.sleep(capped);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void relieveBackpressureIfDrained() {
        if (!flowController.isPaused()) {
            return;
        }
        if (scheduler.deepestQueue() <= properties.dispatch().queueResumeThreshold()) {
            flowController.resumeIfPaused(DomainConstants.Messages.QUEUES_DRAINED);
        }
    }
}
