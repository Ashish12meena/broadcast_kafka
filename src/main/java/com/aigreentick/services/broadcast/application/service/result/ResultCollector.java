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
import org.slf4j.MDC;

import static net.logstash.logback.argument.StructuredArguments.kv;
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
 *
 * <h2>The buffer is not drained until the publish succeeds</h2>
 * Draining first and publishing second means a broker hiccup destroys the only copy of those
 * outcomes: they are already out of the buffer, the exception unwinds, and nothing holds them any
 * more. From the periodic flush that loss is completely silent, because a scheduled task must
 * swallow what it throws to stay alive. The result is the same failure this class was written to
 * prevent — messages that were sent, whose outcome nobody recorded, re-sent later to a customer who
 * already has them.
 *
 * <p>So a failed publish returns the outcomes to the front of their buffer and they go out with the
 * next flush. Order is preserved because the receiving side applies a campaign's results in
 * sequence, and re-publishing something that did reach the broker is harmless — the Messaging
 * Service's callback is idempotent by wamid. Between "possibly twice" and "possibly never", twice
 * is the only safe direction.
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
            @Qualifier(InfraConstants.Executor.SCHEDULER_EXECUTOR) ScheduledExecutorService scheduler) {
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
        ResultKey key = new ResultKey(batch.campaignId(), batch.phoneNumberId());
        flush(key);
        discardIfEmpty(key);

        metrics.batchCompleted(batch.batch().size());
        // kv() rather than interpolation: these become first-class JSON fields, so "durationMs > 5000"
        // is a query instead of a regex over the message text. Same cost, same rendering in the dev
        // console pattern.
        log.info("Batch complete",
                kv(ObservabilityConstants.Logging.MDC_CAMPAIGN_ID, batch.campaignId()),
                kv(ObservabilityConstants.Logging.MDC_PHONE_NUMBER_ID, batch.phoneNumberId()),
                kv("recipients", batch.batch().size()),
                kv("durationMs", batch.ageMs()));

        batch.complete();
    }

    /**
     * Flushes every buffer, and does not let one failure hide the rest.
     *
     * <p>Each key is attempted independently. A single try/catch around the loop would mean the
     * first broker error skipped every campaign after it in the iteration order — buffers that
     * might have published perfectly well, held back for the length of a flush interval by an
     * unrelated failure.
     */
    private void flushAll() {
        for (ResultKey key : List.copyOf(buffers.keySet())) {
            try {
                flush(key);
            } catch (RuntimeException e) {
                // Already logged with campaign context by publish(), and the outcomes are back in
                // their buffer. Swallowed here because a scheduled task that throws is never run
                // again, and this timer is what retries them.
                log.debug("Deferred results for campaignId={} to the next flush", key.campaignId());
            }
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

        try {
            publish(key, readyToSend);
        } catch (RuntimeException e) {
            restore(key, readyToSend);
            throw e;
        }
    }

    /**
     * Puts unpublished outcomes back at the head of their buffer.
     *
     * <p>At the head, not the tail: anything recorded while the publish was in flight is newer, and
     * a campaign's results are applied in order on the receiving side. The buffer is fetched again
     * rather than reused from before the publish, because {@link #discardIfEmpty} may have removed
     * the entry in the meantime.
     */
    private void restore(ResultKey key, List<RecipientOutcome> outcomes) {
        List<RecipientOutcome> buffer = buffers.computeIfAbsent(key, ignored -> new ArrayList<>());
        synchronized (buffer) {
            buffer.addAll(0, outcomes);
        }
    }

    /**
     * Drops the map entry for a campaign that has nothing outstanding.
     *
     * <p>Without this the map keeps one entry per campaign and phone number for the life of the
     * process. Done through {@code computeIfPresent} so the check and the removal happen under the
     * map's own lock: a plain {@code remove} could race a {@link #record} that had just fetched the
     * same buffer, and that thread's outcome would be added to a list no flush can reach.
     */
    private void discardIfEmpty(ResultKey key) {
        buffers.computeIfPresent(key, (ignored, buffer) -> {
            synchronized (buffer) {
                return buffer.isEmpty() ? null : buffer;
            }
        });
    }

    private void publish(ResultKey key, List<RecipientOutcome> outcomes) {
        try {
            publisher.publish(new BatchResult(key.campaignId(), key.phoneNumberId(), outcomes));
            metrics.resultsPublished(outcomes.size());
        } catch (RuntimeException e) {
            metrics.resultsPublishFailed();
            // publish() is reached from the scheduled flush as well as the dispatch path, and the
            // scheduler thread carries no MDC — without this the most important error in this class
            // arrives with no campaign context at all.
            MDC.put(ObservabilityConstants.Logging.MDC_CAMPAIGN_ID, String.valueOf(key.campaignId()));
            MDC.put(ObservabilityConstants.Logging.MDC_PHONE_NUMBER_ID, key.phoneNumberId());
            try {
                log.error("Could not publish results; they remain buffered for the next flush",
                        kv("outcomes", outcomes.size()),
                        kv(ObservabilityConstants.Logging.MDC_CAMPAIGN_ID, key.campaignId()),
                        kv(ObservabilityConstants.Logging.MDC_PHONE_NUMBER_ID, key.phoneNumberId()),
                        e);
            } finally {
                MDC.remove(ObservabilityConstants.Logging.MDC_CAMPAIGN_ID);
                MDC.remove(ObservabilityConstants.Logging.MDC_PHONE_NUMBER_ID);
            }
            throw e;
        }
    }

    /**
     * Last chance to get buffered outcomes to the broker.
     *
     * <p>Anything that fails here is genuinely lost — there is no next flush — so it is logged at
     * error with the count, which is the only trace those recipients will leave.
     */
    @PreDestroy
    void flushOnShutdown() {
        log.info("Flushing buffered results before shutdown");
        flushAll();

        buffers.forEach((key, buffer) -> {
            synchronized (buffer) {
                if (!buffer.isEmpty()) {
                    log.error("Shutting down with unpublished results",
                            kv("outcomes", buffer.size()),
                            kv(ObservabilityConstants.Logging.MDC_CAMPAIGN_ID, key.campaignId()),
                            kv(ObservabilityConstants.Logging.MDC_PHONE_NUMBER_ID, key.phoneNumberId()));
                }
            }
        });
    }

    private record ResultKey(Long campaignId, String phoneNumberId) {
    }
}