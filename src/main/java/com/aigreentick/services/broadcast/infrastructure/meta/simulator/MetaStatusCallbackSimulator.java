package com.aigreentick.services.broadcast.infrastructure.meta.simulator;

import com.aigreentick.services.broadcast.common.constants.DomainConstants;
import com.aigreentick.services.broadcast.common.constants.InfraConstants;
import com.aigreentick.services.broadcast.common.constants.SimulatorConstants;
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
import reactor.core.publisher.Flux;
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
 * <h2>Backpressure, and why waiting is separated from posting</h2>
 * Callbacks are emitted into a sink and drained by a two-stage pipeline: an unbounded stage that
 * does nothing but wait out each callback's delay, then a stage bounded by
 * {@code broadcast.simulator.max-in-flight} that actually posts. Both stages are needed and they
 * bound different things.
 *
 * <p>The bound exists because subscribing per status put no ceiling on concurrency at all: a
 * broadcast of five hundred recipients created fifteen hundred simultaneous requests, which
 * exhausted the connection pool and overflowed Reactor Netty's pending-acquire queue. Every request
 * past the overflow was rejected before it was sent, so those statuses were not delayed — they were
 * silently lost.
 *
 * <p>The separation exists because the delay used to sit inside the bounded stage, which made the
 * bound mean something entirely different from what it says. A callback held one of the
 * {@code max-in-flight} slots for its whole wait, not just for its HTTP call. At the default 32
 * slots and a mean cumulative wait of about nine seconds, the pipeline drained roughly three and a
 * half callbacks per second — for the entire service. A broadcast at Meta's standard 80 messages per
 * second produces two hundred and forty. The sink's 4096-entry buffer filled in under twenty
 * seconds and everything after it was dropped, which presented as "the simulator posts hardly any
 * webhooks" rather than as an error.
 *
 * <p>Waiting is now unbounded because a pending delay costs a timer entry and nothing else — no
 * thread, no connection. Only the posts are bounded, which is the resource that is actually scarce.
 * Throughput is now limited by how fast the receiver answers rather than by how long a simulated
 * network takes to pretend to deliver a message.
 *
 * <p>Keep {@code max-connections} at or above {@code max-in-flight}. They are separate settings and
 * both default to 32; raising in-flight alone puts the pending-acquire overflow back.
 *
 * <p>Fire and forget: a dropped simulated webhook is a missing test status, not lost customer data,
 * so there is no retry and no durability. Statuses still pending when the context closes are
 * abandoned.
 */
@Component
@Profile(InfraConstants.Profile.TEST)
public class MetaStatusCallbackSimulator {

    private static final Logger log = LoggerFactory.getLogger(MetaStatusCallbackSimulator.class);

    private final WebClient callbackWebClient;
    private final MetaSimulatorProperties properties;
    private final AtomicBoolean warnedNoUrl = new AtomicBoolean();

    /**
     * Multicast rather than unicast so the sink tolerates emission from the many virtual threads
     * that run sends concurrently. {@code onBackpressureBuffer} keeps a bounded overflow visible as
     * a rejected emission rather than an unbounded heap of pending callbacks.
     */
    private final Sinks.Many<PendingCallback> pending = Sinks.many().multicast()
            .onBackpressureBuffer(SimulatorConstants.PENDING_CALLBACK_BUFFER, false);

    /** Counted rather than logged per occurrence: under overflow this would be the loudest line. */
    private final AtomicLong dropped = new AtomicLong();

    private Disposable subscription;
    private Disposable reporter;

    /** Read and written only from the reporter's single subscriber thread, so a plain long is fine. */
    private long lastReported;

    public MetaStatusCallbackSimulator(
            @Qualifier(SimulatorConstants.CALLBACK_WEB_CLIENT) WebClient callbackWebClient,
            MetaSimulatorProperties properties) {
        this.callbackWebClient = callbackWebClient;
        this.properties = properties;
    }

    @PostConstruct
    void start() {
        subscription = pending.asFlux()
                // Stage one: wait out the delay. Deliberately unbounded — a pending timer holds no
                // thread and no connection, so there is nothing here worth rationing. This is the
                // stage that must NOT share a budget with the posts; see the class javadoc.
                .flatMap(callback -> Mono.delay(callback.delay()).thenReturn(callback),
                        Integer.MAX_VALUE)
                // Stage two: post. Bounded, because connections are finite and the receiver is not
                // ours to overwhelm.
                .flatMap(this::post, properties.maxInFlight())
                .subscribe();

        reporter = Flux.interval(
                        SimulatorConstants.DROP_REPORT_INTERVAL,
                        SimulatorConstants.DROP_REPORT_INTERVAL)
                .subscribe(tick -> reportDrops());
    }

    @PreDestroy
    void stop() {
        if (reporter != null) {
            reporter.dispose();
        }
        if (subscription != null) {
            subscription.dispose();
        }
        long lost = dropped.get();
        if (lost > 0) {
            log.warn("{} simulated status callbacks were dropped by backpressure this run", lost);
        }
    }

    /**
     * Reports drops periodically while the process runs, not only at shutdown.
     *
     * <p>Reporting only at shutdown is how a saturated simulator reads as a slow one. Someone
     * watching a broadcast sees a trickle of webhooks, no error, and no reason to suspect that most
     * of them were discarded — the number that would have told them arrives after they have stopped
     * looking. Logged only when the count has moved since the last tick, so an idle or healthy run
     * stays silent.
     */
    private void reportDrops() {
        long total = dropped.get();
        long since = total - lastReported;
        if (since <= 0) {
            return;
        }
        lastReported = total;
        log.warn("{} simulated status callbacks dropped by backpressure in the last {}s ({} total). "
                        + "Raise {} and {} together.",
                since, SimulatorConstants.DROP_REPORT_INTERVAL.toSeconds(), total,
                InfraConstants.ConfigKeys.SIMULATOR_MAX_IN_FLIGHT_KEY, InfraConstants.ConfigKeys.SIMULATOR_MAX_CONNECTIONS_KEY);
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
                log.warn("{} is not set; sends are simulated but no delivery statuses will be posted",
                        InfraConstants.ConfigKeys.SIMULATOR_CALLBACK_URL_KEY);
            }
            return;
        }

        try {
            String recipient = normalisePhone(recipientPhone);
            Duration cumulative = Duration.ZERO;

            for (String status : DomainConstants.Meta.STATUS_PROGRESSION) {
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
                        + "(first at wamid={} status={}, reason={}). Consider raising {}.",
                        callback.wamid(), callback.status(), result,
                        InfraConstants.ConfigKeys.SIMULATOR_MAX_IN_FLIGHT_KEY);
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
        // No delay here. The wait happens in the pipeline's first stage, before this Mono is ever
        // subscribed, so a callback that is merely waiting does not hold one of the max-in-flight
        // slots. It used to, and that single line was the reason a broadcast produced a handful of
        // webhooks instead of hundreds — see the class javadoc for the arithmetic.
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
                DomainConstants.Meta.MESSAGING_PRODUCT_WHATSAPP,
                new MetaStatusWebhook.Metadata(phoneNumberId),
                List.of(statusBlock));

        return new MetaStatusWebhook(
                DomainConstants.Meta.WEBHOOK_OBJECT_ACCOUNT,
                List.of(new MetaStatusWebhook.Entry(
                        wabaAccountId == null ? null : String.valueOf(wabaAccountId),
                        List.of(new MetaStatusWebhook.Change(
                                value, DomainConstants.Meta.WEBHOOK_FIELD_MESSAGES)))));
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
            return SimulatorConstants.UNKNOWN_RECIPIENT;
        }
        return phone.replaceAll(SimulatorConstants.NON_DIGIT_PATTERN, "");
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