package com.aigreentick.services.broadcast.infrastructure.observability;

import com.aigreentick.services.broadcast.common.constants.ObservabilityConstants;
import com.aigreentick.services.broadcast.domain.model.CapacitySource;
import com.aigreentick.services.broadcast.domain.policy.MetaErrorClass;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Every metric the service exports, defined in one class.
 *
 * <p>Centralised so that metric names and tags cannot drift, and so the cardinality rules are
 * visible in one place. Phone number is a bounded set and is safe as a tag; campaign identifier is
 * unbounded and is deliberately absent from every metric here — it belongs in logs and traces, where
 * high cardinality costs nothing.
 *
 * <p>The two most important series are {@code broadcast.tokens.*}, whose ratio says how starved the
 * pipeline is, and {@code broadcast.capacity.source}, which is the only signal that the global limit
 * has stopped being enforced across instances.
 */
@Component
public class BroadcastMetrics {

    private final MeterRegistry registry;

    private final Map<String, AtomicInteger> effectiveMps = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> configuredMps = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> capacitySource = new ConcurrentHashMap<>();

    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger queueDepth = new AtomicInteger();
    private final AtomicInteger activeNumbers = new AtomicInteger();
    private final AtomicInteger consumerPaused = new AtomicInteger();
    private final AtomicLong lastCapacityUpdateMs = new AtomicLong();

    public BroadcastMetrics(MeterRegistry registry) {
        this.registry = registry;

        registry.gauge(ObservabilityConstants.Metrics.INFLIGHT, inFlight);
        registry.gauge(ObservabilityConstants.Metrics.QUEUE_DEPTH, queueDepth);
        registry.gauge(ObservabilityConstants.Metrics.QUEUE_ACTIVE_NUMBERS, activeNumbers);
        registry.gauge(ObservabilityConstants.Metrics.CONSUMER_PAUSED, consumerPaused);
        registry.gauge(ObservabilityConstants.Metrics.CAPACITY_LAST_UPDATE_AGE_MS, lastCapacityUpdateMs,
                value -> value.get() == 0 ? 0 : System.currentTimeMillis() - value.get());
    }

    // ----------------------------------------------------------------- rate

    public void tokensRequested(String phoneNumberId, int count) {
        counter(ObservabilityConstants.Metrics.TOKENS_REQUESTED, phoneNumberId).increment(count);
    }

    public void tokensGranted(String phoneNumberId, int count) {
        if (count > 0) {
            counter(ObservabilityConstants.Metrics.TOKENS_GRANTED, phoneNumberId).increment(count);
        }
    }

    public void rateLimitWait(String phoneNumberId, Duration waited) {
        Timer.builder(ObservabilityConstants.Metrics.TOKENS_WAIT)
                .tag(ObservabilityConstants.Metrics.TAG_PHONE_NUMBER_ID, phoneNumberId)
                .register(registry)
                .record(waited);
    }

    // ------------------------------------------------------------- capacity

    public void capacity(String phoneNumberId, int effective, int configured, CapacitySource source) {
        gauge(effectiveMps, ObservabilityConstants.Metrics.CAPACITY_EFFECTIVE_MPS, phoneNumberId).set(effective);
        gauge(configuredMps, ObservabilityConstants.Metrics.CAPACITY_CONFIGURED_MPS, phoneNumberId).set(configured);
        gauge(capacitySource, ObservabilityConstants.Metrics.CAPACITY_SOURCE, phoneNumberId).set(source.ordinal());
        lastCapacityUpdateMs.set(System.currentTimeMillis());
    }

    public void degraded(String phoneNumberId) {
        counter(ObservabilityConstants.Metrics.CAPACITY_DEGRADED, phoneNumberId).increment();
    }

    /**
     * One increment per rate-limiter call served by the local fallback.
     *
     * <p>Deliberately a counter and not a log line: this fires at dispatch-loop frequency during a
     * Redis outage, which is exactly the volume a log pipeline should never be asked to absorb. The
     * corresponding log line is emitted once, on the transition into and out of degradation.
     */
    public void rateLimiterDegraded(String phoneNumberId) {
        counter(ObservabilityConstants.Metrics.RATE_LIMITER_DEGRADED, phoneNumberId).increment();
    }

    /** Dispatch from a number the control plane has never published capacity for. */
    public void capacityUnknown(String phoneNumberId) {
        counter(ObservabilityConstants.Metrics.CAPACITY_UNKNOWN, phoneNumberId).increment();
    }

    // ----------------------------------------------------------------- send

    public void sendStarted() {
        inFlight.incrementAndGet();
    }

    public void sendFinished() {
        inFlight.decrementAndGet();
    }

    public void sendDuration(String phoneNumberId, Duration duration) {
        Timer.builder(ObservabilityConstants.Metrics.SEND_DURATION)
                .tag(ObservabilityConstants.Metrics.TAG_PHONE_NUMBER_ID, phoneNumberId)
                .publishPercentiles(
                        ObservabilityConstants.Metrics.PERCENTILE_P50,
                        ObservabilityConstants.Metrics.PERCENTILE_P95,
                        ObservabilityConstants.Metrics.PERCENTILE_P99)
                .register(registry)
                .record(duration);
    }

    public void sendResult(String phoneNumberId, boolean success, String errorCode) {
        Counter.builder(ObservabilityConstants.Metrics.SEND_RESULT)
                .tag(ObservabilityConstants.Metrics.TAG_PHONE_NUMBER_ID, phoneNumberId)
                .tag(ObservabilityConstants.Metrics.TAG_OUTCOME,
                        success ? ObservabilityConstants.Metrics.OUTCOME_ACCEPTED : ObservabilityConstants.Metrics.OUTCOME_REJECTED)
                .tag(ObservabilityConstants.Metrics.TAG_ERROR_CODE,
                        errorCode == null ? ObservabilityConstants.Metrics.TAG_VALUE_NONE : errorCode)
                .register(registry)
                .increment();
    }

    public void sendClassified(String phoneNumberId, MetaErrorClass errorClass) {
        Counter.builder(ObservabilityConstants.Metrics.SEND_ERROR_CLASS)
                .tag(ObservabilityConstants.Metrics.TAG_PHONE_NUMBER_ID, phoneNumberId)
                .tag(ObservabilityConstants.Metrics.TAG_ERROR_CLASS, errorClass.name())
                .register(registry)
                .increment();
    }

    public void retryScheduled(String phoneNumberId) {
        counter(ObservabilityConstants.Metrics.SEND_RETRY, phoneNumberId).increment();
    }

    public void duplicateSuppressed(String phoneNumberId) {
        counter(ObservabilityConstants.Metrics.SEND_DUPLICATE_SUPPRESSED, phoneNumberId).increment();
    }

    public void circuitRejected(String phoneNumberId) {
        counter(ObservabilityConstants.Metrics.CIRCUIT_REJECTED, phoneNumberId).increment();
    }

    // --------------------------------------------------------------- queues

    public void queueState(int depth, int numbers) {
        queueDepth.set(depth);
        activeNumbers.set(numbers);
    }

    public void consumerPaused(boolean paused) {
        consumerPaused.set(paused ? 1 : 0);
    }

    // -------------------------------------------------------------- results

    public void resultsPublished(int count) {
        registry.counter(ObservabilityConstants.Metrics.RESULTS_PUBLISHED).increment(count);
    }

    public void resultsPublishFailed() {
        registry.counter(ObservabilityConstants.Metrics.RESULTS_PUBLISH_FAILURES).increment();
    }

    public void batchCompleted(int recipients) {
        registry.counter(ObservabilityConstants.Metrics.BATCH_COMPLETED).increment();
        registry.counter(ObservabilityConstants.Metrics.RECIPIENTS_PROCESSED).increment(recipients);
    }

    public void deadLettered(String reason) {
        Counter.builder(ObservabilityConstants.Metrics.DEAD_LETTER)
                .tag(ObservabilityConstants.Metrics.TAG_REASON, reason)
                .register(registry)
                .increment();
    }

    // --------------------------------------------------------------- helpers

    private Counter counter(String name, String phoneNumberId) {
        return Counter.builder(name)
                .tag(ObservabilityConstants.Metrics.TAG_PHONE_NUMBER_ID, phoneNumberId)
                .register(registry);
    }

    private AtomicInteger gauge(Map<String, AtomicInteger> holder, String name, String phoneNumberId) {
        return holder.computeIfAbsent(phoneNumberId, id -> {
            AtomicInteger value = new AtomicInteger();
            registry.gauge(name,
                    io.micrometer.core.instrument.Tags.of(ObservabilityConstants.Metrics.TAG_PHONE_NUMBER_ID, id),
                    value);
            return value;
        });
    }
}
