package com.aigreentick.services.broadcast.application.service.ingest;

import com.aigreentick.services.broadcast.infrastructure.observability.BroadcastMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Pauses and resumes the dispatch consumer to apply backpressure.
 *
 * <h2>Why pause rather than stop, or simply block</h2>
 * Pausing a container stops fetching without leaving the consumer group, so no rebalance happens and
 * no partition is reassigned or redelivered. Stopping the container would trigger a rebalance every
 * time Meta got slow. Blocking the listener thread instead would eventually breach
 * {@code max.poll.interval.ms} and cause the same rebalance by a longer route.
 *
 * <p>The pause and resume thresholds are deliberately far apart. Set close together, the consumer
 * oscillates: it pauses, one batch drains, it resumes, it immediately pauses again.
 */
@Component
public class ConsumerFlowController {

    private static final Logger log = LoggerFactory.getLogger(ConsumerFlowController.class);

    /** Must match the listener id on the dispatch consumer. */
    public static final String DISPATCH_LISTENER_ID = "broadcast-dispatch-listener";

    private final KafkaListenerEndpointRegistry registry;
    private final BroadcastMetrics metrics;
    private final AtomicBoolean paused = new AtomicBoolean(false);

    public ConsumerFlowController(KafkaListenerEndpointRegistry registry, BroadcastMetrics metrics) {
        this.registry = registry;
        this.metrics = metrics;
    }

    public void pauseIfRunning(String reason) {
        MessageListenerContainer container = dispatchContainer();
        if (container == null || container.isContainerPaused()) {
            return;
        }
        if (paused.compareAndSet(false, true)) {
            container.pause();
            metrics.consumerPaused(true);
            log.warn("Dispatch consumer paused: {}", reason);
        }
    }

    public void resumeIfPaused(String reason) {
        MessageListenerContainer container = dispatchContainer();
        if (container == null) {
            return;
        }
        if (paused.compareAndSet(true, false)) {
            container.resume();
            metrics.consumerPaused(false);
            log.info("Dispatch consumer resumed: {}", reason);
        }
    }

    public boolean isPaused() {
        return paused.get();
    }

    private MessageListenerContainer dispatchContainer() {
        return registry.getListenerContainer(DISPATCH_LISTENER_ID);
    }
}
