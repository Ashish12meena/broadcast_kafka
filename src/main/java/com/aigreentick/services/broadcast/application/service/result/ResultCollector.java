package com.aigreentick.services.broadcast.application.service.result;

import com.aigreentick.services.broadcast.common.constants.InfraConstants;
import com.aigreentick.services.broadcast.common.constants.ObservabilityConstants;
import com.aigreentick.services.broadcast.application.port.out.ResultPublisherPort;
import com.aigreentick.services.broadcast.application.service.ingest.InFlightBatch;
import com.aigreentick.services.broadcast.domain.model.BatchResult;
import com.aigreentick.services.broadcast.domain.model.RecipientOutcome;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import com.aigreentick.services.broadcast.infrastructure.observability.BroadcastMetrics;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static net.logstash.logback.argument.StructuredArguments.kv;

/**
 * Groups send outcomes into batches and holds each Kafka batch open until every one of them is
 * durable.
 *
 * <h2>This class has been wrong twice, in opposite directions</h2>
 * The original buffered outcomes in heap and flushed on size or interval — which batched well and
 * could lose data: a flush that failed was logged and dropped while the batch completed anyway, so
 * the offset moved and outcomes for messages the customer had already received were gone.
 *
 * <p>The first rewrite removed the buffer entirely and published one outcome at a time, on the
 * reasoning that the Kafka producer's {@code linger.ms} and {@code batch.size} would supply the
 * batching. That is true at the <em>wire</em> level and irrelevant at the level that matters: each
 * record still carried a single outcome, so the Messaging Service received one record per
 * recipient and opened one database transaction per recipient. A 50,000-recipient campaign became
 * 50,000 records and 50,000 transactions, {@code messaging.campaign.results.apply-chunk-size} had
 * nothing to chunk, and the symptom was a log line per recipient reading {@code recorded=1}.
 *
 * <p>Batching and durability were never actually in tension. What made the original lossy was not
 * the buffer — it was that a failed flush did not stop the offset from moving.
 *
 * <h2>The property that makes buffering safe</h2>
 * An outcome is at risk only while it sits in the buffer, and <strong>the Kafka offset for the
 * batch that produced it is never acknowledged while anything of that batch is buffered or
 * in flight.</strong> {@link #completeBatch} flushes the remainder, joins every outstanding publish
 * future, and only then completes the batch. If any publish failed it throws, the listener does not
 * acknowledge, Kafka redelivers the whole batch, and the idempotency claims suppress the re-sends
 * while the outcomes are reported again.
 *
 * <p>So a crash with a full buffer costs a redelivery, not a loss. That was the one thing the
 * original could not say.
 *
 * <h2>Buffers are keyed by batch, not by campaign and phone number</h2>
 * The original keyed on {@code (campaignId, phoneNumberId)}, which meant two concurrently
 * processing batches for the same campaign shared a buffer and neither could tell which of the
 * pooled outcomes were its own. Keying on the batch keeps the unit of buffering identical to the
 * unit of acknowledgement — one batch, one offset, one set of futures — and removes the question
 * entirely.
 *
 * <p>The cost is smaller batches when several small dispatches for one number are in flight at
 * once. In practice a batch is {@code messaging.campaign.dispatch.batch-size-cap} recipients, far
 * above {@code broadcast.results.flush-size}, so the flush threshold is reached within a single
 * batch and this is the common case rather than the exception.
 */
@Service
public class ResultCollector {

    private static final Logger log = LoggerFactory.getLogger(ResultCollector.class);

    private final ResultPublisherPort publisher;
    private final BroadcastProperties properties;
    private final BroadcastMetrics metrics;
    private final ScheduledExecutorService scheduler;

    private final Map<InFlightBatch, Pending> pending = new ConcurrentHashMap<>();

    public ResultCollector(
            ResultPublisherPort publisher,
            BroadcastProperties properties,
            BroadcastMetrics metrics,
            @Qualifier(InfraConstants.Executor.SCHEDULER_EXECUTOR) ScheduledExecutorService scheduler) {
        this.publisher = publisher;
        this.properties = properties;
        this.metrics = metrics;
        this.scheduler = scheduler;
    }

    /**
     * Flushes partially-filled buffers on a timer.
     *
     * <p>Without this, the last few outcomes of a batch would sit unpublished until the batch
     * completed. That is usually moments later — but a batch whose final recipients are being
     * retried with backoff can stay open for minutes, and the Messaging Service would see no
     * progress for that campaign the whole time.
     */
    @PostConstruct
    void startPeriodicFlush() {
        long intervalMs = properties.results().flushInterval().toMillis();
        scheduler.scheduleWithFixedDelay(
                this::flushAllStale, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Buffers one outcome, publishing when the buffer reaches {@code broadcast.results.flush-size}.
     *
     * <p>Called from send threads, concurrently, for the same batch.
     */
    public void record(InFlightBatch batch, RecipientOutcome outcome) {
        Pending buffer = pending.computeIfAbsent(batch, ignored -> new Pending());

        synchronized (buffer) {
            buffer.outcomes.add(outcome);
            if (buffer.outcomes.size() >= properties.results().flushSize()) {
                drainAndPublish(batch, buffer);
            }
        }
    }

    /**
     * Waits for every outcome in this batch to be durable, then completes it so the offset can
     * move.
     *
     * @throws RuntimeException if any publish failed. The caller must NOT acknowledge the offset —
     *                          that is what turns a broker problem into a redelivery instead of
     *                          silently discarded outcomes
     */
    public void completeBatch(InFlightBatch batch) {
        Pending buffer = pending.remove(batch);

        if (buffer != null) {
            List<CompletableFuture<Void>> futures;

            synchronized (buffer) {
                // Remainder and future list are taken in one critical section, and the buffer is
                // closed inside it. Nothing can slip a publish in between the two.
                drainAndPublish(batch, buffer);
                buffer.closed = true;
                futures = new ArrayList<>(buffer.futures);
            }

            // Joined outside the lock: this waits on the broker and must not hold up a send thread
            // recording into a different batch. join() rather than a timed wait, because the
            // producer's delivery.timeout.ms already bounds how long any one of these can take —
            // a timeout here would be a second, worse copy of that setting. It rethrows on
            // failure, which is the point.
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        }

        metrics.batchCompleted(batch.batch().size());
        log.info("Batch complete",
                kv(ObservabilityConstants.Logging.MDC_CAMPAIGN_ID, batch.campaignId()),
                kv(ObservabilityConstants.Logging.MDC_PHONE_NUMBER_ID, batch.phoneNumberId()),
                kv("traceId", batch.traceId()),
                kv("recipients", batch.batch().size()),
                kv("durationMs", batch.ageMs()));

        batch.complete();
    }

    /**
     * Publishes whatever is buffered and registers the future.
     *
     * <p><strong>Callers must already hold the monitor on {@code buffer}.</strong> The drain, the
     * publish and the future registration are one atomic step.
     *
     * <p>An earlier revision drained under the lock, published outside it, then re-took the lock to
     * register the future. That leaves a window: the periodic flush can publish and be about to
     * register its future exactly as {@code completeBatch} copies the future list.
     * {@code completeBatch} then joins an incomplete set, acknowledges the offset, and if that last
     * publish fails the outcomes are gone with no redelivery to recover them. Microseconds wide,
     * and it would have surfaced as a campaign that occasionally finished a few short — precisely
     * the failure this redesign exists to eliminate.
     *
     * <p>Publishing inside the lock costs nothing: {@code publishAsync} returns immediately and
     * does no I/O on the calling thread.
     */
    private void drainAndPublish(InFlightBatch batch, Pending buffer) {
        if (buffer.outcomes.isEmpty()) {
            return;
        }

        List<RecipientOutcome> outcomes = new ArrayList<>(buffer.outcomes);
        buffer.outcomes.clear();

        CompletableFuture<Void> future = publisher.publishAsync(new BatchResult(
                batch.campaignId(), batch.phoneNumberId(), batch.traceId(), outcomes));

        buffer.futures.add(future);

        future.whenComplete((ignored, error) -> {
            if (error == null) {
                metrics.resultsPublished(outcomes.size());
            } else {
                // Counted here and rethrown by completeBatch. Logging it at both places would
                // double every broker incident in the log; the counter is the alertable signal.
                metrics.resultsPublishFailed();
            }
        });
    }

    /** Publishes anything that has been sitting in a buffer since the last tick. */
    private void flushAllStale() {
        try {
            pending.forEach((batch, buffer) -> {
                synchronized (buffer) {
                    if (buffer.closed) {
                        // completeBatch has taken this one and may already be joining its futures.
                        // Adding a publish now would be a future nobody waits for.
                        return;
                    }
                    drainAndPublish(batch, buffer);
                }
            });
        } catch (RuntimeException e) {
            // Never let the scheduled task die. scheduleWithFixedDelay cancels the whole schedule
            // on an uncaught exception, and the failure mode would be that periodic flushing
            // silently stops for the life of the pod — outcomes would still be published, but only
            // when a buffer filled or a batch completed.
            log.error("Periodic result flush failed; will retry on the next tick", e);
        }
    }

    /**
     * Publishes what is buffered on the way down and waits for it.
     *
     * <p>Unlike the original, a failure here is not a loss. Every batch still holding outcomes has
     * an unacknowledged offset, so anything that does not make it to the broker is redelivered when
     * the partition is reassigned.
     */
    @PreDestroy
    void flushOnShutdown() {
        log.info("Flushing results for {} in-flight batch(es) before shutdown", pending.size());

        pending.forEach((batch, buffer) -> {
            List<CompletableFuture<Void>> futures;
            synchronized (buffer) {
                drainAndPublish(batch, buffer);
                buffer.closed = true;
                futures = new ArrayList<>(buffer.futures);
            }
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            } catch (RuntimeException e) {
                log.warn("Results for campaignId={} did not reach the broker before shutdown; "
                                + "the offset was not acknowledged and Kafka will redeliver",
                        batch.campaignId());
            }
        });
        pending.clear();
    }

    /**
     * One batch's unpublished outcomes and its outstanding publishes.
     *
     * <p>No internal synchronisation: every field is touched only while the caller holds the
     * monitor on the enclosing instance. A {@code synchronizedList} here would look safer and would
     * not be — the invariant that matters spans several fields, and per-field locking cannot
     * express it.
     */
    private static final class Pending {
        private final List<RecipientOutcome> outcomes = new ArrayList<>();
        private final List<CompletableFuture<Void>> futures = new ArrayList<>();

        /** Set once {@code completeBatch} or shutdown has taken this buffer. */
        private boolean closed;
    }
}
