package com.aigreentick.services.broadcast.application.service.dispatch;

import com.aigreentick.services.broadcast.application.port.out.RateLimiterPort;
import com.aigreentick.services.broadcast.application.service.ingest.ConsumerFlowController;
import com.aigreentick.services.broadcast.application.service.ingest.PendingSend;
import com.aigreentick.services.broadcast.application.service.ingest.PhoneNumberQueue;
import com.aigreentick.services.broadcast.domain.model.RateGrant;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import com.aigreentick.services.broadcast.infrastructure.observability.BroadcastMetrics;
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
 * {@code windowSize} per second — with a window of eighty and a two-second tail latency, forty
 * messages per second against an eighty message per second allowance. The barrier was the
 * bottleneck, not the limit.
 *
 * <h2>Asking for only what is wanted</h2>
 * The request is {@code min(chunkSize, pendingRecipients)}. Asking for a full chunk when only ten
 * recipients remain would spend tokens that are then thrown away, and those tokens are the number's
 * real capacity.
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

    DispatchWorker(
            PhoneNumberQueue queue,
            RateLimiterPort rateLimiter,
            SendExecutor sendExecutor,
            DispatchScheduler scheduler,
            ConsumerFlowController flowController,
            BroadcastProperties properties,
            BroadcastMetrics metrics) {
        this.queue = queue;
        this.rateLimiter = rateLimiter;
        this.sendExecutor = sendExecutor;
        this.scheduler = scheduler;
        this.flowController = flowController;
        this.properties = properties;
        this.metrics = metrics;
    }

    @Override
    public void run() {
        String phoneNumberId = queue.phoneNumberId();
        log.debug("Dispatch worker started phoneNumberId={}", phoneNumberId);

        try {
            while (!scheduler.isShuttingDown()) {
                if (queue.isEmpty() && shouldExit()) {
                    return;
                }

                int wanted = Math.min(properties.dispatch().chunkSize(), queue.pendingRecipients());
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
                    // Submitted and not awaited. The next token acquisition happens while these are
                    // still in flight, which is what keeps the rate continuous.
                    sendExecutor.submit(send);
                }

                relieveBackpressureIfDrained();
            }
        } catch (RuntimeException e) {
            log.error("Dispatch worker failed phoneNumberId={}", phoneNumberId, e);
        } finally {
            queue.forceStopWorker();
            // Work may have arrived between the last check and releasing the flag. Nothing else will
            // notice, so this worker restarts the queue itself.
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
        // Something arrived. Take the flag back if nobody else has.
        return !queue.tryStartWorker();
    }

    private void sleepFor(long waitMicros, String phoneNumberId) {
        long millis = Math.max(1, waitMicros / 1_000);
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
            flowController.resumeIfPaused("queues drained below the resume threshold");
        }
    }
}
