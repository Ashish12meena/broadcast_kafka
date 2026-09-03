package com.aigreentick.services.broadcast.infrastructure.meta.simulator;

import com.aigreentick.services.broadcast.infrastructure.meta.simulator.dto.MetaStatusWebhook;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Posts simulated delivery status webhooks for messages the simulator accepted.
 *
 * <p>Every message walks the same progression Meta uses — {@code sent}, {@code delivered},
 * {@code read} — and each status waits its own random draw <em>after</em> the previous one. The
 * randomness is what makes messages overtake each other, as they do in production; the fact that the
 * draws are cumulative rather than independent is what keeps a single message's own statuses in
 * order, since {@code read} arriving before {@code delivered} is something Meta never does.
 *
 * <p>{@code phoneNumberId} and {@code wabaAccountId} both come from the {@code DispatchEvent} that
 * produced the send, so nothing about the sending account is configured.
 *
 * <h2>Backpressure</h2>
 * Callbacks are emitted into a sink and posted through a {@code flatMap} bounded by
 * {@code broadcast.simulator.max-in-flight}, rather than each subscribing independently. Subscribing
 * per status put no ceiling on concurrency: a broadcast of five hundred recipients created fifteen
 * hundred simultaneous requests, which exhausted the connection pool and overflowed Reactor Netty's
 * pending-acquire queue. Every request past the overflow was rejected before it was sent, so the
 * statuses were not delayed — they were silently lost. Bounding concurrency here means a burst
 * queues in a place that can be measured, and the pool is never asked for more than it has.
 *
 * <p>Fire and forget: a dropped simulated webhook is a missing test status, not lost customer data,
 * so there is no retry and no durability. Statuses still pending when the context closes are
 * abandoned.
 */
@Component
@Profile("test")
public class MetaStatusCallbackSimulator {

    private static final Logger log = LoggerFactory.getLogger(MetaStatusCallbackSimulator.class);

    private static final List<String> PROGRESSION = List.of("sent", "delivered", "read");

    private final WebClient callbackWebClient;
    private final MetaSimulatorProperties properties;
    private final AtomicBoolean warnedNoUrl = new AtomicBoolean();

    /**
     * Multicast rather than unicast so the sink tolerates emission from the many virtual threads
     * that run sends concurrently. {@code onBackpressureBuffer} keeps a bounded overflow visible as
     * a rejected emission rather than an unbounded heap of pending callbacks.
     */
    private final Sinks.Many<PendingCallback> pending =
            Sinks.many().multicast().onBackpressureBuffer(4096, false);

    /** Counted rather than logged per occurrence: under overflow this would be the loudest line. */
    private final AtomicLong dropped = new AtomicLong();

    private Disposable subscription;

    public MetaStatusCallbackSimulator(
            @Qualifier("simulatorCallbackWebClient") WebClient callbackWebClient,
            MetaSimulatorProperties properties) {
        this.callbackWebClient = callbackWebClient;
        this.properties = properties;
    }

    @PostConstruct
    void start() {
        subscription = pending.asFlux()
                .flatMap(this::post, properties.maxInFlight())
                .subscribe();
    }

    @PreDestroy
    void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
        long lost = dropped.get();
        if (lost > 0) {
            log.warn("{} simulated status callbacks were dropped by backpressure this run", lost);
        }
    }

    /**
     * Schedules the status progression for one accepted message.
     *
     * <p>Returns immediately and never throws: this runs on the send path, and an exception here
     * would fail a send the simulator has already reported as accepted.
     *
     * @param callbackData whatever was in {@code biz_opaque_callback_data} on the send, echoed back
     *                     on every status exactly as read. Deliberately opaque: Messaging Service's
     *                     correlation format is {@code msg:<messageId>} today, and parsing or
     *                     rebuilding that prefix here would tie their format to a broadcast release.
     *                     Null until their renderer sets it, which serializes to an absent key
     */
    public void scheduleFor(
            String phoneNumberId,
            Long wabaAccountId,
            String wamid,
            String recipientPhone,
            String callbackData) {

        if (!properties.callbacksEnabled()) {
            if (warnedNoUrl.compareAndSet(false, true)) {
                log.warn("broadcast.simulator.callback-url is not set; sends are simulated but no "
                        + "delivery statuses will be posted");
            }
            return;
        }

        try {
            String recipient = normalisePhone(recipientPhone);
            Duration cumulative = Duration.ZERO;

            for (String status : PROGRESSION) {
                cumulative = cumulative.plus(
                        randomDelay(properties.minDelay(), properties.maxDelay()));
                emit(new PendingCallback(
                        phoneNumberId, wabaAccountId, wamid, recipient, status, callbackData,
                        cumulative));
            }
        } catch (RuntimeException e) {
            log.error("Could not schedule simulated statuses wamid={}", wamid, e);
        }
    }

    private void emit(PendingCallback callback) {
        Sinks.EmitResult result = pending.tryEmitNext(callback);
        if (result.isFailure()) {
            // Backpressure, not an error worth failing a send over. Counted and reported once at
            // shutdown so a saturated run is visible without a wall of identical warnings.
            long total = dropped.incrementAndGet();
            if (total == 1) {
                log.warn("Simulated status callbacks are being dropped by backpressure "
                        + "(first at wamid={} status={}, reason={}). Consider raising "
                        + "broadcast.simulator.max-in-flight.",
                        callback.wamid(), callback.status(), result);
            }
        }
    }

    private Mono<Void> post(PendingCallback callback) {
        return Mono.defer(() -> {
            // Stamped here rather than at scheduling time. Stamping at schedule time gave all three
            // statuses for a message an identical timestamp despite arriving seconds apart, so no
            // consumer's ordering or rank-guard logic was ever exercised by simulated traffic.
            MetaStatusWebhook payload = buildPayload(
                    callback.phoneNumberId(),
                    callback.wabaAccountId(),
                    callback.wamid(),
                    callback.recipient(),
                    callback.status(),
                    callback.callbackData(),
                    Instant.now());

            return callbackWebClient.post()
                    // URI.create rather than the String overload: the configured URL is opaque, and
                    // a brace in it would otherwise be read as a template placeholder.
                    .uri(URI.create(properties.callbackUrl()))
                    .headers(headers -> properties.headers().forEach(headers::add))
                    .bodyValue(payload)
                    .retrieve()
                    .toBodilessEntity()
                    .doOnNext(ignored -> log.debug(
                            "Simulated status posted wamid={} status={} afterMs={}",
                            callback.wamid(), callback.status(), callback.delay().toMillis()))
                    .then();
        })
        // delaySubscription, not a scheduled task: the wait costs a timer entry rather than a
        // thread. Using the shared schedulerExecutor would put slow HTTP calls on the two platform
        // threads that also run retry re-queues and result flushes.
        .delaySubscription(callback.delay())
        // Kept inside flatMap so one failure never cancels the shared subscription. Without this,
        // a single rejected callback would terminate the pipeline and silently stop every
        // subsequent status for the lifetime of the process.
        .onErrorResume(error -> {
            log.warn("Simulated status callback failed wamid={} status={} reason={}",
                    callback.wamid(), callback.status(), error.toString());
            return Mono.empty();
        });
    }

    MetaStatusWebhook buildPayload(
            String phoneNumberId,
            Long wabaAccountId,
            String wamid,
            String recipient,
            String status,
            String callbackData,
            Instant at) {

        String timestamp = String.valueOf(at.getEpochSecond());

        MetaStatusWebhook.Status statusBlock =
                new MetaStatusWebhook.Status(wamid, status, timestamp, recipient, callbackData);

        MetaStatusWebhook.Value value = new MetaStatusWebhook.Value(
                "whatsapp",
                new MetaStatusWebhook.Metadata(phoneNumberId),
                List.of(statusBlock));

        return new MetaStatusWebhook(
                "whatsapp_business_account",
                List.of(new MetaStatusWebhook.Entry(
                        wabaAccountId == null ? null : String.valueOf(wabaAccountId),
                        List.of(new MetaStatusWebhook.Change(value, "messages")))));
    }

    static Duration randomDelay(Duration min, Duration max) {
        long minMs = min.toMillis();
        long maxMs = max.toMillis();
        if (maxMs <= minMs) {
            return Duration.ofMillis(minMs);
        }
        return Duration.ofMillis(ThreadLocalRandom.current().nextLong(minMs, maxMs + 1));
    }

    /** Meta reports recipients in international format with no plus sign. */
    static String normalisePhone(String phone) {
        if (phone == null || phone.isBlank()) {
            return "0000000000";
        }
        return phone.replaceAll("[^0-9]", "");
    }

    private record PendingCallback(
            String phoneNumberId,
            Long wabaAccountId,
            String wamid,
            String recipient,
            String status,
            String callbackData,
            Duration delay) {
    }
}