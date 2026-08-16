package com.aigreentick.services.broadcast.application.service.result;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Buffers outcomes and publishes them in groups.
 *
 * <h2>Grouped rather than one at a time</h2>
 * A thousand recipients reported individually is a thousand events and a thousand transactions on
 * the receiving side to record what a handful can. The Messaging Service's callback handler is
 * already written to apply a batch in one transaction.
 *
 * <h2>Flushed before the batch is acknowledged</h2>
 * {@link #completeBatch} publishes whatever is buffered for that campaign before running the
 * completion callback, and the callback is what commits the Kafka offset. Acknowledging first would
 * mean a crash in between loses outcomes for messages that were genuinely sent — and the recovery
 * path for a message with no recorded outcome is to send it again, to a customer who already has it.
 */
@Service
public class ResultCollector {

    private static final Logger log = LoggerFactory.getLogger(ResultCollector.class);

    private final ResultPublisherPort publisher;
    private final BroadcastProperties properties;
    private final BroadcastMetrics metrics;
    private final ScheduledExecutorService scheduler;

    /** Outcomes waiting to be published, grouped by campaign and phone number. */
    private final Map<ResultKey, List<RecipientOutcome>> buffers = new ConcurrentHashMap<>();

    public ResultCollector(
            ResultPublisherPort publisher,
            BroadcastProperties properties,
            BroadcastMetrics metrics,
            @Qualifier("schedulerExecutor") ScheduledExecutorService scheduler) {
        this.publisher = publisher;
        this.properties = properties;
        this.metrics = metrics;
        this.scheduler = scheduler;
    }

    @PostConstruct
    void startPeriodicFlush() {
        long intervalMs = properties.results().flushInterval().toMillis();
        scheduler.scheduleWithFixedDelay(
                this::flushAll, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void record(InFlightBatch batch, RecipientOutcome outcome) {
        ResultKey key = new ResultKey(batch.campaignId(), batch.phoneNumberId());

        List<RecipientOutcome> readyToSend = null;
        List<RecipientOutcome> buffer = buffers.computeIfAbsent(key, ignored -> new ArrayList<>());

        synchronized (buffer) {
            buffer.add(outcome);
            if (buffer.size() >= properties.results().flushSize()) {
                readyToSend = new ArrayList<>(buffer);
                buffer.clear();
            }
        }

        if (readyToSend != null) {
            publish(key, readyToSend);
        }
    }

    /** Publishes everything outstanding for a batch, then completes it so the offset can move. */
    public void completeBatch(InFlightBatch batch) {
        flush(new ResultKey(batch.campaignId(), batch.phoneNumberId()));

        metrics.batchCompleted(batch.batch().size());
        log.info("Batch complete campaignId={} phoneNumberId={} recipients={} durationMs={}",
                batch.campaignId(), batch.phoneNumberId(), batch.batch().size(), batch.ageMs());

        batch.complete();
    }

    private void flushAll() {
        try {
            for (ResultKey key : List.copyOf(buffers.keySet())) {
                flush(key);
            }
        } catch (RuntimeException e) {
            // A scheduled task that throws is never run again. Swallowing here keeps the timer alive.
            log.error("Periodic result flush failed", e);
        }
    }

    private void flush(ResultKey key) {
        List<RecipientOutcome> buffer = buffers.get(key);
        if (buffer == null) {
            return;
        }

        List<RecipientOutcome> readyToSend;
        synchronized (buffer) {
            if (buffer.isEmpty()) {
                return;
            }
            readyToSend = new ArrayList<>(buffer);
            buffer.clear();
        }
        publish(key, readyToSend);
    }

    private void publish(ResultKey key, List<RecipientOutcome> outcomes) {
        try {
            publisher.publish(new BatchResult(key.campaignId(), key.phoneNumberId(), outcomes));
            metrics.resultsPublished(outcomes.size());
        } catch (RuntimeException e) {
            metrics.resultsPublishFailed();
            log.error("Could not publish {} results campaignId={} phoneNumberId={}",
                    outcomes.size(), key.campaignId(), key.phoneNumberId(), e);
            throw e;
        }
    }

    @PreDestroy
    void flushOnShutdown() {
        log.info("Flushing buffered results before shutdown");
        flushAll();
    }

    private record ResultKey(Long campaignId, String phoneNumberId) {
    }
}
