package com.aigreentick.services.broadcast.application.service.dispatch;

import com.aigreentick.services.broadcast.application.port.out.RateLimiterPort;
import com.aigreentick.services.broadcast.application.service.ingest.ConsumerFlowController;
import com.aigreentick.services.broadcast.application.service.ingest.PhoneNumberQueue;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import com.aigreentick.services.broadcast.infrastructure.observability.BroadcastMetrics;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Builds a worker for a queue.
 *
 * <p>Workers are per-queue objects with their own state, so they cannot be Spring beans. This
 * factory holds the collaborators they need and hands them over at construction, which keeps the
 * worker itself free of any dependency lookup.
 */
@Component
public class DispatchWorkerFactory {

    private final RateLimiterPort rateLimiter;
    private final SendExecutor sendExecutor;
    private final DispatchScheduler scheduler;
    private final ConsumerFlowController flowController;
    private final BroadcastProperties properties;
    private final BroadcastMetrics metrics;

    public DispatchWorkerFactory(
            RateLimiterPort rateLimiter,
            SendExecutor sendExecutor,
            // The scheduler creates workers and workers ask the scheduler to resume queues, so one
            // of the two references has to be resolved late.
            @Lazy DispatchScheduler scheduler,
            ConsumerFlowController flowController,
            BroadcastProperties properties,
            BroadcastMetrics metrics) {
        this.rateLimiter = rateLimiter;
        this.sendExecutor = sendExecutor;
        this.scheduler = scheduler;
        this.flowController = flowController;
        this.properties = properties;
        this.metrics = metrics;
    }

    public DispatchWorker create(PhoneNumberQueue queue) {
        return new DispatchWorker(
                queue, rateLimiter, sendExecutor, scheduler, flowController, properties, metrics);
    }
}
