package com.aigreentick.services.broadcast.infrastructure.meta.simulator;

import com.aigreentick.services.broadcast.infrastructure.meta.simulator.dto.MetaStatusWebhook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

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

    public MetaStatusCallbackSimulator(
            @Qualifier("simulatorCallbackWebClient") WebClient callbackWebClient,
            MetaSimulatorProperties properties) {
        this.callbackWebClient = callbackWebClient;
        this.properties = properties;
    }

    /**
     * Schedules the status progression for one accepted message.
     *
     * <p>Returns immediately and never throws: this runs on the send path, and an exception here
     * would fail a send the simulator has already reported as accepted.
     */
    public void scheduleFor(
            String phoneNumberId, Long wabaAccountId, String wamid, String recipientPhone) {

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
                post(buildPayload(phoneNumberId, wabaAccountId, wamid, recipient, status),
                        status, wamid, cumulative);
            }
        } catch (RuntimeException e) {
            log.error("Could not schedule simulated statuses wamid={}", wamid, e);
        }
    }

    private void post(MetaStatusWebhook payload, String status, String wamid, Duration delay) {
        callbackWebClient.post()
                // URI.create rather than the String overload: the configured URL is opaque, and a
                // brace in it would otherwise be read as a template placeholder.
                .uri(URI.create(properties.callbackUrl()))
                .headers(headers -> properties.headers().forEach(headers::add))
                .bodyValue(payload)
                .retrieve()
                .toBodilessEntity()
                // delaySubscription, not a scheduled task: the wait costs a timer entry rather than
                // a thread. Using the shared schedulerExecutor would put slow HTTP calls on the two
                // platform threads that also run retry re-queues and result flushes.
                .delaySubscription(delay)
                .subscribe(
                        ignored -> log.debug("Simulated status posted wamid={} status={} afterMs={}",
                                wamid, status, delay.toMillis()),
                        error -> log.warn("Simulated status callback failed wamid={} status={} "
                                + "reason={}", wamid, status, error.toString()));
    }

    MetaStatusWebhook buildPayload(
            String phoneNumberId, Long wabaAccountId, String wamid, String recipient, String status) {

        String timestamp = String.valueOf(Instant.now().getEpochSecond());

        MetaStatusWebhook.Status statusBlock =
                new MetaStatusWebhook.Status(wamid, status, timestamp, recipient);

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
}
