package com.aigreentick.services.broadcast.application.service.result;

import com.aigreentick.services.broadcast.common.constants.ObservabilityConstants;
import com.aigreentick.services.broadcast.application.port.out.ResultPublisherPort;
import com.aigreentick.services.broadcast.application.service.ingest.InFlightBatch;
import com.aigreentick.services.broadcast.domain.model.BatchResult;
import com.aigreentick.services.broadcast.domain.model.RecipientOutcome;
import com.aigreentick.services.broadcast.infrastructure.observability.BroadcastMetrics;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.logstash.logback.argument.StructuredArguments.kv;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;

/**
 * Publishes outcomes as they happen and holds the batch open until the broker has them all.
 *
 * <h2>What changed, and why the heap buffer had to go</h2>
 * This class used to accumulate outcomes in a {@code ConcurrentHashMap} of {@code ArrayList}s and
 * flush them on a size threshold or a timer. That buffer held the only record of messages
 * <strong>the customer had already received</strong>, in memory, on one pod. A SIGKILL, an OOM or a
 * node eviction lost them, and its own shutdown hook said so: "anything that fails here is
 * genuinely lost — there is no next flush."
 *
 * <p>The recovery for a lost outcome was Kafka redelivery plus the Redis idempotency claim, which
 * works — until the claim TTL has expired or Redis was down when the claim was taken, at which
 * point the recipient is sent to twice. So the duplicate-delivery risk was a race between a TTL and
 * consumer-group rebalance latency, and nothing measured either.
 *
 * <p>The buffer is gone. Each outcome is published immediately and the producer's own
 * {@code linger.ms} and {@code batch.size} do the batching — same wire efficiency, durable the
 * moment the broker acknowledges. The batch is completed, and its offset acknowledged, only after
 * every outstanding publish future has resolved.
 *
 * <h2>The offset still moves last</h2>
 * That property was right before and is right now. Kafka has no partial acknowledgement, so
 * committing early discards the recipients that had not been sent yet. What is different is that
 * "every recipient has an outcome" and "every outcome is durable" are now the same statement
 * rather than two things separated by a flush interval.
 *
 * <h2>Failure is loud, not silent</h2>
 * If a publish future fails, {@link #completeBatch} throws and the caller leaves the offset
 * uncommitted — Kafka redelivers, the idempotency claims suppress the re-sends, and the outcomes
 * are reported again. Between "possibly twice" and "possibly never", twice is the only safe
 * direction, and now it is also the only one that can happen.
 */
@Service
public class ResultCollector {

    private static final Logger log = LoggerFactory.getLogger(ResultCollector.class);

    private final ResultPublisherPort publisher;
    private final BroadcastMetrics metrics;

    /**
     * Publishes still in flight, per batch.
     *
     * <p>Keyed on the batch rather than on the campaign: the thing that must not be acknowledged
     * early is one Kafka record, and one record is one batch. Keying on the campaign would make a
     * long-running campaign's first batch wait for its last.
     */
    private final Map<InFlightBatch, List<CompletableFuture<Void>>> inFlight =
            new ConcurrentHashMap<>();

    public ResultCollector(ResultPublisherPort publisher, BroadcastMetrics metrics) {
        this.publisher = publisher;
        this.metrics = metrics;
    }

    /**
     * Publishes one outcome and remembers the future.
     *
     * <p>Asynchronous: this runs on a send thread and must not block it on a broker round trip.
     * The future is what {@link #completeBatch} waits on.
     */
    public void record(InFlightBatch batch, RecipientOutcome outcome) {
        CompletableFuture<Void> future = publisher.publishAsync(new BatchResult(
                batch.campaignId(), batch.phoneNumberId(), batch.traceId(), List.of(outcome)));

        inFlight.computeIfAbsent(batch, ignored ->
                java.util.Collections.synchronizedList(new java.util.ArrayList<>())).add(future);

        future.whenComplete((ignored, error) -> {
            if (error == null) {
                metrics.resultsPublished(1);
            } else {
                metrics.resultsPublishFailed();
            }
        });
    }

    /**
     * Waits for every outcome in this batch to be durable, then completes it so the offset can
     * move.
     *
     * @throws RuntimeException when any publish failed. The caller must not acknowledge the offset
     */
    public void completeBatch(InFlightBatch batch) {
        List<CompletableFuture<Void>> futures = inFlight.remove(batch);

        if (futures != null && !futures.isEmpty()) {
            CompletableFuture<Void> all;
            synchronized (futures) {
                all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            }
            // Joins rather than blocks indefinitely: the producer's own delivery.timeout.ms bounds
            // how long any one of these can take, so this inherits a bound rather than needing its
            // own. A timeout here would just be a second, worse copy of that setting.
            all.join();
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
     * Waits for outstanding publishes on the way down.
     *
     * <p>Anything still in flight here belongs to a batch whose offset was never acknowledged, so
     * a failure costs a redelivery rather than a lost outcome — which is exactly the property the
     * old heap buffer could not offer.
     */
    @PreDestroy
    void awaitOutstanding() {
        log.info("Waiting for {} batch(es) of results to reach the broker", inFlight.size());
        inFlight.forEach((batch, futures) -> {
            try {
                synchronized (futures) {
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                }
            } catch (RuntimeException e) {
                log.warn("Results for campaignId={} did not reach the broker before shutdown; "
                                + "the offset was not acknowledged and Kafka will redeliver",
                        batch.campaignId());
            }
        });
        inFlight.clear();
    }
}
