package com.aigreentick.services.broadcast.application.service.dispatch;

import com.aigreentick.services.broadcast.common.constants.DomainConstants;
import com.aigreentick.services.broadcast.common.constants.InfraConstants;
import com.aigreentick.services.broadcast.common.constants.ObservabilityConstants;
import com.aigreentick.services.broadcast.application.port.out.IdempotencyPort;
import com.aigreentick.services.broadcast.application.port.out.MetaSendPort;
import com.aigreentick.services.broadcast.application.service.capacity.CapacityDegrader;
import com.aigreentick.services.broadcast.application.service.ingest.PendingSend;
import com.aigreentick.services.broadcast.application.service.result.ResultCollector;
import com.aigreentick.services.broadcast.domain.model.Recipient;
import com.aigreentick.services.broadcast.domain.model.RecipientOutcome;
import com.aigreentick.services.broadcast.domain.model.SendResponse;
import com.aigreentick.services.broadcast.domain.policy.MetaErrorCatalog;
import com.aigreentick.services.broadcast.domain.policy.MetaErrorClass;
import com.aigreentick.services.broadcast.domain.policy.RetryPolicy;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import com.aigreentick.services.broadcast.infrastructure.observability.BroadcastMetrics;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Performs one send and decides what happens next.
 *
 * <h2>Concurrency is bounded here, rate is not</h2>
 * The semaphore caps how many sends this instance has in flight at once. It is not the rate limit —
 * that lives in Redis and is shared — it is a local bulkhead so one pod cannot exhaust its own
 * sockets. A local limit could never enforce the rate correctly anyway: it is right on one instance
 * and wrong on two.
 *
 * <h2>The token arrives with the batch</h2>
 * {@code DispatchBatch} carries it, fetched once per batch by the Messaging Service. Nothing is
 * cached here, so a rotation takes effect on the next batch rather than needing invalidation — see
 * {@code DispatchEvent} for why the credential travels over Kafka and what it would take to change
 * that.
 *
 * <h2>Retries go back through the meter</h2>
 * A retryable failure is put back on the queue rather than re-sent from here. It therefore has to
 * acquire tokens again like any other send, which makes a retry storm impossible by construction:
 * retrying costs capacity, so a number cannot exceed its rate by failing.
 */
@Service
public class SendExecutor {

    private static final Logger log = LoggerFactory.getLogger(SendExecutor.class);

    /** One breaker per number, because Meta being unreachable for one says nothing about others. */
    private static final String CIRCUIT_BREAKER_NAME_PREFIX = "meta-";

    private final MetaSendPort metaSend;
    private final IdempotencyPort idempotency;
    private final ResultCollector resultCollector;
    private final CapacityDegrader degrader;
    private final DispatchScheduler scheduler;
    private final CircuitBreakerRegistry circuitBreakers;
    private final BroadcastMetrics metrics;
    private final BroadcastProperties properties;
    private final RetryPolicy retryPolicy;
    private final ExecutorService dispatchExecutor;
    private final ScheduledExecutorService scheduledExecutor;
    private final Semaphore inFlightPermits;

    public SendExecutor(
            MetaSendPort metaSend,
            IdempotencyPort idempotency,
            ResultCollector resultCollector,
            CapacityDegrader degrader,
            @Lazy DispatchScheduler scheduler,
            CircuitBreakerRegistry circuitBreakers,
            BroadcastMetrics metrics,
            BroadcastProperties properties,
            @Qualifier(InfraConstants.Executor.DISPATCH_EXECUTOR) ExecutorService dispatchExecutor,
            @Qualifier(InfraConstants.Executor.SCHEDULER_EXECUTOR)
            ScheduledExecutorService scheduledExecutor) {
        this.metaSend = metaSend;
        this.idempotency = idempotency;
        this.resultCollector = resultCollector;
        this.degrader = degrader;
        this.scheduler = scheduler;
        this.circuitBreakers = circuitBreakers;
        this.metrics = metrics;
        this.properties = properties;
        this.dispatchExecutor = dispatchExecutor;
        this.scheduledExecutor = scheduledExecutor;
        this.retryPolicy = new RetryPolicy(
                properties.retry().baseBackoff(),
                properties.retry().maxBackoff(),
                properties.retry().maxAttempts());
        this.inFlightPermits = new Semaphore(properties.dispatch().maxConcurrentSends());
    }

    /** Hands the send to a virtual thread. Returns without waiting for it. */
    public void submit(PendingSend send) {
        dispatchExecutor.submit(() -> execute(send));
    }

    private void execute(PendingSend send) {
        Recipient recipient = send.recipient();
        String phoneNumberId = send.phoneNumberId();

        Map<String, String> priorContext = MDC.getCopyOfContextMap();

        MDC.put(ObservabilityConstants.Logging.MDC_CAMPAIGN_ID,
                String.valueOf(send.batch().campaignId()));
        MDC.put(ObservabilityConstants.Logging.MDC_PHONE_NUMBER_ID, phoneNumberId);
        MDC.put(ObservabilityConstants.Logging.MDC_RECIPIENT_ID,
                String.valueOf(recipient.recipientId()));
        if (send.batch().traceId() != null) {
            MDC.put(ObservabilityConstants.Logging.MDC_TRACE_ID, send.batch().traceId());
        }

        boolean permitHeld = false;
        try {
            inFlightPermits.acquire();
            permitHeld = true;

            if (send.attempts() == 0 && !idempotency.claim(recipient.recipientId())) {
                // Already sent by an earlier delivery of this batch, so Meta must not be called
                // again. The outcome is still reported, and that is the correction: reporting
                // nothing left the Messaging Service holding a row in PROCESSING with no result
                // ever arriving, which the stuck sweep eventually released and re-sent — producing
                // the exact duplicate this guard exists to prevent, by way of the guard.
                metrics.duplicateSuppressed(phoneNumberId);
                String priorMessageId = idempotency.claimedMessageId(recipient.recipientId());
                // DEBUG, not INFO: rare in steady state, but a consumer rebalance redelivers the
                // whole batch and this then fires once per recipient — thousands of lines at the
                // worst possible moment. The counter above is the signal worth alerting on.
                log.debug("Duplicate suppressed; recipient was already dispatched wamid={}",
                        priorMessageId);
                resolve(send, RecipientOutcome.accepted(recipient, priorMessageId,
                        DomainConstants.Meta.STATUS_ACCEPTED, send.attempts()));
                return;
            }

            int attempt = send.recordAttempt();
            SendResponse response = callMeta(send);
            handle(send, response, attempt);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            resolve(send, RecipientOutcome.failed(recipient, DomainConstants.ErrorCodes.INTERRUPTED,
                    DomainConstants.Messages.SEND_INTERRUPTED, true, send.attempts()));
        } catch (RuntimeException e) {
            log.error("Unexpected failure while sending", e);
            resolve(send, RecipientOutcome.failed(recipient,
                    DomainConstants.ErrorCodes.INTERNAL_ERROR, e.getMessage(), true,
                    send.attempts()));
        } finally {
            if (permitHeld) {
                inFlightPermits.release();
            }
            if (priorContext == null) {
                MDC.clear();
            } else {
                MDC.setContextMap(priorContext);
            }
        }
    }

    private SendResponse callMeta(PendingSend send) {
        String phoneNumberId = send.phoneNumberId();
        CircuitBreaker breaker = circuitBreakers.circuitBreaker(
                CIRCUIT_BREAKER_NAME_PREFIX + phoneNumberId);

        if (!breaker.tryAcquirePermission()) {
            metrics.circuitRejected(phoneNumberId);
            return SendResponse.unreachable(DomainConstants.Messages.CIRCUIT_OPEN);
        }

        long startNanos = System.nanoTime();
        metrics.sendStarted();
        try {
            SendResponse response = metaSend.send(
                    phoneNumberId,
                    send.batch().batch().wabaAccountId(),
                    send.batch().batch().accessToken(),
                    send.recipient().requestPayload());

            Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);
            metrics.sendDuration(phoneNumberId, elapsed);

            if (response.transportFailure()) {
                // Only unreachability counts against the breaker. A business rejection means Meta
                // is healthy and answering, and tripping on those would stop a working number.
                breaker.onError(elapsed.toNanos(), TimeUnit.NANOSECONDS,
                        new IOException(response.errorMessage()));
            } else {
                breaker.onSuccess(elapsed.toNanos(), TimeUnit.NANOSECONDS);
            }
            return response;

        } finally {
            metrics.sendFinished();
        }
    }

    private void handle(PendingSend send, SendResponse response, int attempt) {
        Recipient recipient = send.recipient();
        String phoneNumberId = send.phoneNumberId();

        if (response.success()) {
            idempotency.confirm(recipient.recipientId(), response.providerMessageId());
            metrics.sendResult(phoneNumberId, true, null);
            resolve(send, RecipientOutcome.accepted(recipient, response.providerMessageId(),
                    response.messageStatus(), attempt));
            return;
        }

        MetaErrorClass errorClass = response.transportFailure()
                ? MetaErrorClass.TRANSIENT
                : MetaErrorCatalog.classify(response.errorCode());

        String errorCode = response.errorCode() == null
                ? (response.transportFailure()
                        ? DomainConstants.ErrorCodes.TRANSPORT : DomainConstants.ErrorCodes.UNKNOWN)
                : String.valueOf(response.errorCode());

        metrics.sendResult(phoneNumberId, false, errorCode);
        metrics.sendClassified(phoneNumberId, errorClass);

        switch (errorClass) {
            case RATE_LIMIT -> {
                // The number is over its limit, so slow the number down rather than this message.
                // Handling it as an ordinary per-message backoff would leave every other worker
                // pushing the same number further over the limit.
                degrader.degradeAfterRateLimit(phoneNumberId);
                retryOrFail(send, errorCode, response.errorMessage(), attempt, Duration.ZERO);
            }
            case UPGRADE_IN_PROGRESS -> {
                // The number is briefly unusable while Meta upgrades its throughput. Not a fault,
                // and emphatically not a reason to reduce its rate.
                degrader.suppressForUpgrade(phoneNumberId);
                retryOrFail(send, errorCode, response.errorMessage(), attempt, Duration.ZERO);
            }
            case PAIR_RATE_LIMIT ->
                // Specific to this recipient. Degrading the number would throttle thousands of
                // unrelated recipients over one person's message frequency.
                retryOrFail(send, errorCode, response.errorMessage(), attempt,
                        retryPolicy.delayFor(attempt));
            case TRANSIENT ->
                retryOrFail(send, errorCode, response.errorMessage(), attempt,
                        retryPolicy.delayFor(attempt));
            case CREDENTIAL -> {
                // Retrying the payload cannot help; the token is the problem. Reported retryable
                // so the Messaging Service re-dispatches with a freshly fetched token — nothing is
                // cached here, so the next batch already carries a new one.
                log.error("Meta rejected the access token errorCode={} message={}",
                        errorCode, response.errorMessage());
                resolve(send, RecipientOutcome.failed(recipient, errorCode,
                        response.errorMessage(), true, attempt));
            }
            case PERMANENT -> {
                idempotency.confirm(recipient.recipientId(), null);
                resolve(send, RecipientOutcome.failed(recipient, errorCode,
                        response.errorMessage(), false, attempt));
            }
        }
    }

    private void retryOrFail(PendingSend send, String errorCode, String errorMessage,
                             int attempt, Duration delay) {
        if (!retryPolicy.shouldRetry(attempt)) {
            log.warn("Giving up after {} attempts errorCode={} message={}",
                    attempt, errorCode, errorMessage);
            resolve(send, RecipientOutcome.failed(send.recipient(), errorCode, errorMessage,
                    true, attempt));
            return;
        }

        metrics.retryScheduled(send.phoneNumberId());
        // Released so a retry is not blocked by its own earlier claim. The window in which a
        // duplicate could slip through is the retry delay, and the queue holds only this
        // instance's copy of the work.
        idempotency.release(send.recipient().recipientId());

        if (delay.isZero() || delay.isNegative()) {
            scheduler.requeue(send);
        } else {
            scheduledExecutor.schedule(() -> scheduler.requeue(send),
                    delay.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Records the outcome and, if this was the batch's last recipient, completes the batch.
     *
     * @param outcome null when the recipient needs no report
     */
    private void resolve(PendingSend send, RecipientOutcome outcome) {
        if (outcome != null) {
            resultCollector.record(send.batch(), outcome);
        }
        if (!send.batch().recordResolved()) {
            return;
        }
        try {
            resultCollector.completeBatch(send.batch());
        } catch (RuntimeException e) {
            // completeBatch joins the outstanding publish futures and rethrows if any failed.
            // Letting that escape was a silent-loss path: called from execute()'s try block it
            // landed in the catch below, which called resolve() a second time, decremented an
            // already-zero counter, and threw again from inside a catch — so the batch's outcomes
            // were lost, the offset was never acknowledged, and nothing above the executor ever
            // heard about it.
            //
            // Logged here and swallowed deliberately: the offset stays uncommitted, so Kafka
            // redelivers the whole batch and the idempotency claims make the redelivery safe.
            log.error("Could not complete batch campaignId={} phoneNumberId={} traceId={}; the "
                            + "offset will not be acknowledged and Kafka will redeliver",
                    send.batch().campaignId(), send.phoneNumberId(), send.batch().traceId(), e);
        }
    }

    /**
     * Waits for in-flight sends to finish.
     *
     * <p>Acquiring every permit is how it waits: once all of them are held, nothing is in flight.
     */
    @PreDestroy
    public void awaitInFlight() {
        int total = properties.dispatch().maxConcurrentSends();
        long graceMillis = properties.dispatch().shutdownGrace().toMillis();
        try {
            if (inFlightPermits.tryAcquire(total, graceMillis, TimeUnit.MILLISECONDS)) {
                log.info("All in-flight sends completed before shutdown");
            } else {
                log.warn("Shutdown grace of {}ms expired with {} sends still in flight",
                        graceMillis, total - inFlightPermits.availablePermits());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
