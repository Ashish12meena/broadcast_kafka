package com.aigreentick.services.broadcast.infrastructure.observability;

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

        registry.gauge("broadcast.inflight", inFlight);
        registry.gauge("broadcast.queue.depth", queueDepth);
        registry.gauge("broadcast.queue.active_numbers", activeNumbers);
        registry.gauge("broadcast.consumer.paused", consumerPaused);
        registry.gauge("broadcast.capacity.last_update_age_ms", lastCapacityUpdateMs,
                value -> value.get() == 0 ? 0 : System.currentTimeMillis() - value.get());
    }

    // ----------------------------------------------------------------- rate

    public void tokensRequested(String phoneNumberId, int count) {
        counter("broadcast.tokens.requested", phoneNumberId).increment(count);
    }

    public void tokensGranted(String phoneNumberId, int count) {
        if (count > 0) {
            counter("broadcast.tokens.granted", phoneNumberId).increment(count);
        }
    }

    public void rateLimitWait(String phoneNumberId, Duration waited) {
        Timer.builder("broadcast.tokens.wait")
                .tag("phone_number_id", phoneNumberId)
                .register(registry)
                .record(waited);
    }

    // ------------------------------------------------------------- capacity

    public void capacity(String phoneNumberId, int effective, int configured, CapacitySource source) {
        gauge(effectiveMps, "broadcast.capacity.effective_mps", phoneNumberId).set(effective);
        gauge(configuredMps, "broadcast.capacity.configured_mps", phoneNumberId).set(configured);
        gauge(capacitySource, "broadcast.capacity.source", phoneNumberId).set(source.ordinal());
        lastCapacityUpdateMs.set(System.currentTimeMillis());
    }

    public void degraded(String phoneNumberId) {
        counter("broadcast.capacity.degraded", phoneNumberId).increment();
    }

    // ----------------------------------------------------------------- send

    public void sendStarted() {
        inFlight.incrementAndGet();
    }

    public void sendFinished() {
        inFlight.decrementAndGet();
    }

    public void sendDuration(String phoneNumberId, Duration duration) {
        Timer.builder("broadcast.send.duration")
                .tag("phone_number_id", phoneNumberId)
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry)
                .record(duration);
    }

    public void sendResult(String phoneNumberId, boolean success, String errorCode) {
        Counter.builder("broadcast.send.result")
                .tag("phone_number_id", phoneNumberId)
                .tag("outcome", success ? "accepted" : "rejected")
                .tag("error_code", errorCode == null ? "none" : errorCode)
                .register(registry)
                .increment();
    }

    public void sendClassified(String phoneNumberId, MetaErrorClass errorClass) {
        Counter.builder("broadcast.send.error_class")
                .tag("phone_number_id", phoneNumberId)
                .tag("class", errorClass.name())
                .register(registry)
                .increment();
    }

    public void retryScheduled(String phoneNumberId) {
        counter("broadcast.send.retry", phoneNumberId).increment();
    }

    public void duplicateSuppressed(String phoneNumberId) {
        counter("broadcast.send.duplicate_suppressed", phoneNumberId).increment();
    }

    public void circuitRejected(String phoneNumberId) {
        counter("broadcast.circuit.rejected", phoneNumberId).increment();
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
        registry.counter("broadcast.results.published").increment(count);
    }

    public void resultsPublishFailed() {
        registry.counter("broadcast.results.publish.failures").increment();
    }

    public void batchCompleted(int recipients) {
        registry.counter("broadcast.batch.completed").increment();
        registry.counter("broadcast.recipients.processed").increment(recipients);
    }

    public void deadLettered(String reason) {
        Counter.builder("broadcast.dead_letter")
                .tag("reason", reason)
                .register(registry)
                .increment();
    }

    // --------------------------------------------------------------- helpers

    private Counter counter(String name, String phoneNumberId) {
        return Counter.builder(name).tag("phone_number_id", phoneNumberId).register(registry);
    }

    private AtomicInteger gauge(Map<String, AtomicInteger> holder, String name, String phoneNumberId) {
        return holder.computeIfAbsent(phoneNumberId, id -> {
            AtomicInteger value = new AtomicInteger();
            registry.gauge(name, io.micrometer.core.instrument.Tags.of("phone_number_id", id), value);
            return value;
        });
    }
}
