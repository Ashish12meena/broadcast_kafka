package com.aigreentick.services.broadcast.common.constants;

/**
 * Names this service is observed by: metric names and tags, health detail keys, and MDC keys.
 *
 * <p>All of it is a contract with dashboards, alerts and log queries built outside this repository,
 * which is what makes these worth holding in one place — renaming any of them breaks something that
 * the compiler cannot see.
 */
public final class ObservabilityConstants {

    private ObservabilityConstants() {
    }

    /**
     * Names and tag keys for every metric the service exports.
     *
     * <p>A metric name is a contract with the dashboards and alerts built on it, so renaming one is a
     * change with consequences outside this repository. Holding the names here makes that visible and
     * makes the cardinality rule easy to check: phone number is a bounded set and is safe as a tag,
     * campaign identifier is unbounded and appears on no metric.
     */
    public static final class Metrics {

        private Metrics() {
        }

        // ------------------------------------------------------------------- tag keys

        public static final String TAG_PHONE_NUMBER_ID = "phone_number_id";
        public static final String TAG_OUTCOME = "outcome";
        public static final String TAG_ERROR_CODE = "error_code";
        public static final String TAG_ERROR_CLASS = "class";
        public static final String TAG_REASON = "reason";

        // ----------------------------------------------------------------- tag values

        public static final String OUTCOME_ACCEPTED = "accepted";
        public static final String OUTCOME_REJECTED = "rejected";

        /** Used where a metric requires a tag value but none applies. */
        public static final String TAG_VALUE_NONE = "none";

        // ------------------------------------------------------------------- gauges

        public static final String INFLIGHT = "broadcast.inflight";
        public static final String QUEUE_DEPTH = "broadcast.queue.depth";
        public static final String QUEUE_ACTIVE_NUMBERS = "broadcast.queue.active_numbers";
        public static final String CONSUMER_PAUSED = "broadcast.consumer.paused";
        public static final String CAPACITY_LAST_UPDATE_AGE_MS = "broadcast.capacity.last_update_age_ms";

        // --------------------------------------------------------------------- rate

        /** Requested against granted is the ratio that says how starved the pipeline is. */
        public static final String TOKENS_REQUESTED = "broadcast.tokens.requested";
        public static final String TOKENS_GRANTED = "broadcast.tokens.granted";
        public static final String TOKENS_WAIT = "broadcast.tokens.wait";

        // ----------------------------------------------------------------- capacity

        public static final String CAPACITY_EFFECTIVE_MPS = "broadcast.capacity.effective_mps";
        public static final String CAPACITY_CONFIGURED_MPS = "broadcast.capacity.configured_mps";

        /** The only signal that the global limit has stopped being enforced across instances. */
        public static final String CAPACITY_SOURCE = "broadcast.capacity.source";
        public static final String CAPACITY_DEGRADED = "broadcast.capacity.degraded";

        // --------------------------------------------------------------------- send

        public static final String SEND_DURATION = "broadcast.send.duration";
        public static final String SEND_RESULT = "broadcast.send.result";
        public static final String SEND_ERROR_CLASS = "broadcast.send.error_class";
        public static final String SEND_RETRY = "broadcast.send.retry";
        public static final String SEND_DUPLICATE_SUPPRESSED = "broadcast.send.duplicate_suppressed";
        public static final String CIRCUIT_REJECTED = "broadcast.circuit.rejected";

        // ------------------------------------------------------------------ results

        public static final String RESULTS_PUBLISHED = "broadcast.results.published";
        public static final String RESULTS_PUBLISH_FAILURES = "broadcast.results.publish.failures";
        public static final String BATCH_COMPLETED = "broadcast.batch.completed";
        public static final String RECIPIENTS_PROCESSED = "broadcast.recipients.processed";
        public static final String DEAD_LETTER = "broadcast.dead_letter";

        // --------------------------------------------------------------- percentiles

        public static final double PERCENTILE_P50 = 0.5;
        public static final double PERCENTILE_P95 = 0.95;
        public static final double PERCENTILE_P99 = 0.99;

        // ------------------------------------------------------------- health details

        public static final String HEALTH_KNOWN_PHONE_NUMBERS = "knownPhoneNumbers";
        public static final String HEALTH_DEGRADED_PHONE_NUMBERS = "degradedPhoneNumbers";
        public static final String HEALTH_PHONE_NUMBERS_ON_LOCAL_FALLBACK = "phoneNumbersOnLocalFallback";

        // ---------------------------------------------------------- dead letter reasons

        /** Bounded set, so the metric tag cannot take one value per exception message. */
        public static final String DEAD_LETTER_REASON_DESERIALIZATION = "deserialization";
        public static final String DEAD_LETTER_REASON_VALIDATION = "validation";
        public static final String DEAD_LETTER_REASON_UNKNOWN = "unknown";
    }

    /**
     * MDC keys attached to log lines.
     *
     * <p>These are what a log query is written against, so they have to be spelled identically on the
     * consumer thread that sets some of them and the send thread that sets the rest. Naming them once
     * removes the class of bug where {@code phoneNumberId} and {@code phone_number_id} both appear and
     * half the lines fall out of a search.
     */
    public static final class Logging {

        private Logging() {
        }

        public static final String MDC_CAMPAIGN_ID = "campaignId";
        public static final String MDC_PHONE_NUMBER_ID = "phoneNumberId";
        public static final String MDC_RECIPIENT_ID = "recipientId";
        public static final String MDC_KAFKA_KEY = "kafkaKey";
        public static final String MDC_PARTITION = "partition";
        public static final String MDC_OFFSET = "offset";
    }
}
