package com.aigreentick.services.broadcast.common.constants;

import java.util.List;

/**
 * Values that carry business meaning: Meta's protocol literals, the fixed numbers in the pacing
 * algorithm, the error codes this service reports, and the messages that travel outside it.
 *
 * <p>The dividing line against {@link InfraConstants} is whether the value would still exist if the
 * transport changed. A Meta message status and a retry's halving factor would; a Kafka header name
 * would not.
 */
public final class DomainConstants {

    private DomainConstants() {
    }

    /**
     * Literals defined by Meta's Cloud API contract: message statuses, throughput tier names, request
     * and webhook field names.
     *
     * <p>These change only when Meta changes them, which is exactly what makes them worth naming in one
     * place — a Graph version bump becomes a review of this class rather than a search through the
     * codebase.
     *
     * <p>Meta's numeric error codes are not here. They belong with the classification logic that gives
     * them meaning, in
     * {@link com.aigreentick.services.broadcast.domain.policy.MetaErrorCatalog}, and duplicating them
     * would create the drift that catalog exists to prevent.
     */
    public static final class Meta {

        private Meta() {
        }

        // ------------------------------------------------------------- message statuses

        /** What Meta reports when it has accepted a send but not yet delivered it. */
        public static final String STATUS_ACCEPTED = "accepted";

        public static final String STATUS_SENT = "sent";
        public static final String STATUS_DELIVERED = "delivered";
        public static final String STATUS_READ = "read";

        /**
         * The progression every message walks, in order. Simulated statuses follow it so a receiver's
         * ordering logic sees the same sequence production produces.
         */
        public static final List<String> STATUS_PROGRESSION =
                List.of(STATUS_SENT, STATUS_DELIVERED, STATUS_READ);

        // --------------------------------------------------------------- throughput tiers

        public static final String TIER_STANDARD = "STANDARD";
        public static final String TIER_HIGH_THROUGHPUT = "HIGH_THROUGHPUT";
        public static final String TIER_HIGH_THROUGHPUT_HYPHENATED = "HIGH-THROUGHPUT";
        public static final String TIER_HIGH = "HIGH";
        public static final String TIER_COEXISTENCE = "COEXISTENCE";
        public static final String TIER_CO_EXISTENCE = "CO_EXISTENCE";

        /** Placeholder tier for a number nothing has been published about. */
        public static final String TIER_UNKNOWN = "UNKNOWN";

        // ------------------------------------------------------------- envelope literals

        public static final String MESSAGING_PRODUCT_WHATSAPP = "whatsapp";
        public static final String WEBHOOK_OBJECT_ACCOUNT = "whatsapp_business_account";
        public static final String WEBHOOK_FIELD_MESSAGES = "messages";

        // ------------------------------------------------------- request payload fields

        /** Recipient number in the rendered send body. */
        public static final String PAYLOAD_FIELD_TO = "to";

        /** Opaque correlation value echoed back on every status. Never parsed by this service. */
        public static final String PAYLOAD_FIELD_CALLBACK_DATA = "biz_opaque_callback_data";

        // ------------------------------------------------------------------ diagnostics

        /** How much of an unreadable Meta response body is kept in a log line. */
        public static final int RESPONSE_BODY_LOG_LIMIT = 512;

        public static final String TRUNCATION_SUFFIX = "...";

        /** Largest response this service will buffer in memory from Meta. */
        public static final int MAX_IN_MEMORY_RESPONSE_BYTES = 256 * 1024;

        /** Name of the Meta connection pool, which is what its metrics are tagged with. */
        public static final String CONNECTION_POOL_NAME = "meta";
    }

    /**
     * Fixed values in the pacing and capacity logic.
     *
     * <p>Everything tunable per deployment lives in {@code broadcast.*} instead. What is here is
     * arithmetic that is part of the algorithm rather than of a configuration: the halving step, the
     * floor a rate may never drop below, and unit conversions.
     */
    public static final class Dispatch {

        private Dispatch() {
        }

        /**
         * The floor for any rate. Zero would suspend a number permanently with no event able to revive
         * it, since recovery is driven by the Messaging Service raising a rate that is still moving.
         */
        public static final int MIN_EFFECTIVE_MPS = 1;

        /**
         * A rate limit halves the number's rate. Fast down and slow up is the standard shape for a
         * control loop that must not oscillate; recovery is the Messaging Service's job, on a timer.
         */
        public static final int DEGRADE_DIVISOR = 2;

        /** Shortest a worker will sleep when the meter says to wait, so it cannot spin. */
        public static final long MIN_SLEEP_MILLIS = 1;

        public static final long MICROS_PER_MILLI = 1_000L;
        public static final long MICROS_PER_SECOND = 1_000_000L;
        public static final double NANOS_PER_SECOND = 1_000_000_000d;

        /** Initial capacity for a drain, kept modest since a chunk is normally far smaller. */
        public static final int DRAIN_LIST_INITIAL_CAPACITY = 128;

        /**
         * Ceiling on the exponent in the retry backoff, so the shift cannot overflow a long on a
         * misconfigured attempt count.
         */
        public static final int MAX_BACKOFF_SHIFT = 16;

        /** A batch with no recipients is acknowledged rather than dispatched. */
        public static final int EMPTY_BATCH_SIZE = 0;
    }

    /**
     * Error codes this service reports when Meta did not supply one.
     *
     * <p>They travel to the Messaging Service on {@code RecipientOutcome.errorCode} and end up in a
     * database column and on a metric tag, so they are part of an external contract rather than
     * incidental log text.
     */
    public static final class ErrorCodes {

        private ErrorCodes() {
        }

        /** The send was interrupted, normally by shutdown. Reported retryable. */
        public static final String INTERRUPTED = "INTERRUPTED";

        /** A fault in this service rather than in the message or in Meta. */
        public static final String INTERNAL_ERROR = "INTERNAL_ERROR";

        /** Meta could not be reached, so there is no code from Meta to report. */
        public static final String TRANSPORT = "TRANSPORT";

        /** Meta answered but supplied no code. */
        public static final String UNKNOWN = "UNKNOWN";
    }

    /**
     * Messages that leave this service or are reused across classes.
     *
     * <p>Deliberately not a home for every log line. A log message written once, at the site that
     * produces it, is easier to read in place than as an indirection. What is collected here is text
     * that travels — dead letter reasons an operator reads, error text reported back to the Messaging
     * Service, and the pause and resume reasons that appear on both sides of the backpressure decision.
     */
    public static final class Messages {

        private Messages() {
        }

        // ------------------------------------------------------- dispatch event validation

        /** Reasons a dispatch event is set aside rather than processed. Read by a human on the DLQ. */
        public static final String VALIDATION_CAMPAIGN_ID_MISSING = "campaignId is missing";
        public static final String VALIDATION_PHONE_NUMBER_ID_MISSING = "phoneNumberId is missing";
        public static final String VALIDATION_ACCESS_TOKEN_MISSING = "accessToken is missing";
        public static final String VALIDATION_PAYLOADS_EMPTY = "payloads is empty";
        public static final String VALIDATION_PAYLOAD_BODY_MISSING =
                "one or more payloads have no request body";

        /** Prefixed to the exception text when an event cannot be read at all. */
        public static final String DESERIALIZATION_FAILED_PREFIX = "deserialization failed: ";

        // -------------------------------------------------------------------- send errors

        public static final String CIRCUIT_OPEN = "Circuit open for this phone number";
        public static final String SEND_INTERRUPTED = "Send interrupted during shutdown";
        public static final String EMPTY_META_RESPONSE = "Empty response from Meta";
        public static final String META_RETURNED_NOTHING_USABLE =
                "Meta returned neither a message nor an error";

        /** Takes the HTTP status Meta answered with. */
        public static final String META_RETURNED_STATUS_FORMAT = "Meta returned %s";

        // ------------------------------------------------------------------ backpressure

        /** Takes the observed depth and the configured limit. */
        public static final String QUEUE_DEPTH_LIMIT_REACHED_FORMAT =
                "queue depth %d reached the limit of %d";

        public static final String QUEUES_DRAINED = "queues drained below the resume threshold";

        // ------------------------------------------------------------ result publishing

        public static final String RESULT_PUBLISH_INTERRUPTED = "Interrupted while publishing results";

        /** Takes the campaign identifier. */
        public static final String RESULT_PUBLISH_FAILED_FORMAT =
                "Could not publish results for campaign %s";
    }
}
