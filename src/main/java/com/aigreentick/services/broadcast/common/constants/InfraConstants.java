package com.aigreentick.services.broadcast.common.constants;

import java.time.Duration;

/**
 * Values that describe how this service talks to its infrastructure: Kafka, Redis, the thread pools
 * and the Spring profile that decides whether sends are real.
 *
 * <p>Grouped into nested classes rather than split across files so a call site needs one import for
 * the whole layer, and so the group names carry meaning at the point of use —
 * {@code InfraConstants.Redis.CAPACITY_TTL} says more than a bare {@code CAPACITY_TTL} would.
 *
 * <p>Nothing environment-specific belongs here. Broker addresses, topic names and Redis hosts arrive
 * through configuration; what is written down here is the shape of the contract, not the deployment.
 */
public final class InfraConstants {

    private InfraConstants() {
    }

    /**
     * Kafka wiring values: listener identifiers, container factory bean names, client settings, dead
     * letter header names and topic provisioning defaults.
     *
     * <p>Topic <em>names</em> are not here. They are environment configuration and belong to
     * {@code broadcast.topics.*}; only values that are part of how this service talks to Kafka live in
     * this class.
     */
    public static final class Kafka {

        private Kafka() {
        }

        // --------------------------------------------------------------- listener ids

        /**
         * Identifies the dispatch listener container so the flow controller can pause and resume it.
         * Referenced both by the {@code @KafkaListener} declaration and by the lookup that pauses it —
         * they must be the same value or backpressure silently stops working.
         */
        public static final String DISPATCH_LISTENER_ID = "broadcast-dispatch-listener";

        public static final String CAPACITY_LISTENER_ID = "broadcast-capacity-listener";

        // ------------------------------------------------------- container factory beans

        public static final String DISPATCH_LISTENER_FACTORY = "dispatchListenerFactory";
        public static final String CAPACITY_LISTENER_FACTORY = "capacityListenerFactory";

        // ------------------------------------------------------------- consumer settings

        /**
         * Suffix for the capacity consumer's group. Every instance needs every capacity update, so each
         * one reads the whole compacted topic under a group of its own rather than sharing partitions.
         */
        public static final String CAPACITY_GROUP_SUFFIX = "-capacity-";

        public static final String AUTO_OFFSET_RESET_EARLIEST = "earliest";
        public static final int FETCH_MAX_WAIT_MS = 500;

        /** One consumer thread is enough: the capacity topic is low volume and order matters per key. */
        public static final int CAPACITY_CONCURRENCY = 1;

        // ------------------------------------------------------------- producer settings

        public static final String ACKS_ALL = "all";
        public static final String COMPRESSION_SNAPPY = "snappy";
        public static final int DELIVERY_TIMEOUT_MS = 120_000;
        public static final int MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 5;
        public static final int LINGER_MS = 20;

        // ------------------------------------------------------------ publish timeouts

        /** How long to wait for the broker when setting a message aside. */
        public static final long DEAD_LETTER_PUBLISH_TIMEOUT_SECONDS = 10;

        /**
         * How long to wait for the broker when publishing outcomes. Longer than the dead letter timeout
         * because the caller acknowledges a Kafka offset on the strength of this having succeeded.
         */
        public static final long RESULT_PUBLISH_TIMEOUT_SECONDS = 30;

        // ---------------------------------------------------------- dead letter headers

        public static final String HEADER_DLQ_REASON = "dlq-reason";
        public static final String HEADER_DLQ_SOURCE_TOPIC = "dlq-source-topic";
        public static final String HEADER_DLQ_PARTITION = "dlq-partition";
        public static final String HEADER_DLQ_OFFSET = "dlq-offset";
        public static final String HEADER_DLQ_TIMESTAMP = "dlq-timestamp";

        /** Source topic label recorded on a dead letter from the dispatch stream. */
        public static final String SOURCE_TOPIC_OUTBOUND_MESSAGES = "outbound-messages";

        // ------------------------------------------------------------ topic provisioning

        /**
         * Divisible by 2, 3, 4, 6, 8 and 12, so instance counts in that range each get an equal share of
         * partitions rather than an uneven split where one pod does twice the work.
         */
        public static final int DISPATCH_PARTITIONS = 24;

        /** Low-volume topics: capacity updates and dead letters. */
        public static final int LOW_VOLUME_PARTITIONS = 3;

        public static final int DEFAULT_REPLICAS = 1;

        public static final String CONFIG_CLEANUP_POLICY = "cleanup.policy";
        public static final String CONFIG_MIN_CLEANABLE_DIRTY_RATIO = "min.cleanable.dirty.ratio";
        public static final String CONFIG_SEGMENT_MS = "segment.ms";
        public static final String CONFIG_RETENTION_MS = "retention.ms";

        public static final String CLEANUP_POLICY_COMPACT = "compact";
        public static final String MIN_CLEANABLE_DIRTY_RATIO = "0.1";
        public static final String CAPACITY_SEGMENT_MS = "60000";

        /** Long retention: a dead letter is investigated by a human, on human timescales. */
        public static final long DEAD_LETTER_RETENTION_MS = 30L * 24 * 60 * 60 * 1000;
    }

    /**
     * Redis key shapes, hash field names, TTLs and script values.
     *
     * <p>The hash field names are shared with {@code redis/token_bucket.lua}, which reads
     * {@link #FIELD_EFFECTIVE_MPS} and {@link #FIELD_BACKOFF_UNTIL_MS} from the capacity hash. Lua
     * cannot import from here, so a change to either name has to be made in both places — naming them
     * once on the Java side at least makes the Java half a single edit.
     *
     * <p>Keys are assembled by {@link com.aigreentick.services.broadcast.infrastructure.redis.RedisKeys}
     * from the prefixes below rather than being written out literally there.
     */
    public static final class Redis {

        private Redis() {
        }

        // ---------------------------------------------------------------- key prefixes

        /**
         * The braces are a Redis Cluster hash tag, not decoration. The token bucket script reads the
         * capacity hash and writes the bucket hash in one call, and Redis Cluster rejects a script whose
         * keys live on different slots — the shared tag guarantees they do not.
         */
        public static final String HASH_TAG_OPEN = "{";
        public static final String HASH_TAG_CLOSE = "}";

        /** Capacity for one number: effective and configured rate, tier, backoff. */
        public static final String CAPACITY_KEY_PREFIX = "wa:cap:";

        /** Token bucket state for one number. */
        public static final String TOKEN_BUCKET_KEY_PREFIX = "wa:tb:";

        /** Short-lived lock collapsing a burst of rate-limit responses into one degrade. */
        public static final String DEGRADE_LOCK_KEY_PREFIX = "wa:degradelock:";

        /** Duplicate-send guard for one recipient. */
        public static final String SENT_CLAIM_KEY_PREFIX = "wa:sent:";

        // ------------------------------------------------------ capacity hash fields

        public static final String FIELD_CONFIGURED_MPS = "configuredMps";
        public static final String FIELD_EFFECTIVE_MPS = "effectiveMps";
        public static final String FIELD_TIER = "tier";
        public static final String FIELD_BACKOFF_UNTIL_MS = "backoffUntilMs";
        public static final String FIELD_UPDATED_AT_MS = "updatedAtMs";
        public static final String FIELD_SOURCE = "source";

        // --------------------------------------------------------------------- values

        /**
         * Marker written when a recipient is claimed but not yet confirmed. Replaced by the wamid on a
         * successful send, so a suppressed duplicate can still report the original message id.
         */
        public static final String CLAIM_MARKER = "CLAIMED";

        /** The degrade lock carries no information; only its presence matters. */
        public static final String DEGRADE_LOCK_VALUE = "1";

        // ----------------------------------------------------------------------- TTLs

        /**
         * How long a capacity hash survives without an update. Long enough that a quiet number keeps its
         * rate overnight, short enough that a decommissioned number does not linger indefinitely.
         */
        public static final Duration CAPACITY_TTL = Duration.ofHours(24);

        /** Token bucket state is cheap to rebuild, so it need not outlive a quiet hour. */
        public static final long TOKEN_BUCKET_TTL_SECONDS = 3_600;

        // --------------------------------------------------------------------- script

        /** Classpath location of the token bucket script. */
        public static final String TOKEN_BUCKET_SCRIPT_PATH = "redis/token_bucket.lua";

        /** The script's signal that no capacity has been published for a number. */
        public static final long CAPACITY_UNKNOWN = -1L;

        /** Number of values the token bucket script returns: {@code granted} and {@code waitMicros}. */
        public static final int TOKEN_BUCKET_RESULT_SIZE = 2;

        public static final int TOKEN_BUCKET_RESULT_GRANTED_INDEX = 0;
        public static final int TOKEN_BUCKET_RESULT_WAIT_INDEX = 1;
    }

    /**
     * Executor bean names, thread name prefixes and pool sizes.
     *
     * <p>The bean names matter most: they are declared once on {@code @Bean} and repeated on every
     * {@code @Qualifier} that injects them, and a mismatch is a startup failure rather than something
     * the compiler catches.
     */
    public static final class Executor {

        private Executor() {
        }

        /** Virtual-thread executor running one drain loop per active phone number, plus their sends. */
        public static final String DISPATCH_EXECUTOR = "dispatchExecutor";

        /** Platform-thread executor for timed work: retry re-queues and result flushes. */
        public static final String SCHEDULER_EXECUTOR = "schedulerExecutor";

        public static final String DISPATCH_THREAD_PREFIX = "dispatch-";
        public static final String SCHEDULER_THREAD_PREFIX = "broadcast-sched-";

        /** Only a handful of timed tasks exist, and each is short. */
        public static final int SCHEDULER_POOL_SIZE = 2;

        public static final String DESTROY_METHOD_CLOSE = "close";
        public static final String DESTROY_METHOD_SHUTDOWN_NOW = "shutdownNow";
    }

    /**
     * Spring profile names.
     *
     * <p>The profile is the whole safety mechanism around simulated sending: under {@link #TEST} the
     * real Meta client is not registered at all, so there is no code path from the dispatch loop to the
     * network. That guarantee rests on these two strings agreeing, which is reason enough to write them
     * once.
     */
    public static final class Profile {

        private Profile() {
        }

        /** Sends are simulated. No message reaches WhatsApp. */
        public static final String TEST = "test";

        /** Every profile but {@link #TEST}; the real Meta client is registered only here. */
        public static final String NOT_TEST = "!test";
    }

    /**
     * Configuration property keys and the {@code ${...}} placeholder expressions used in annotations.
     *
     * <p>Annotations need compile-time constants, so a property key referenced from {@code @Value},
     * {@code @KafkaListener} or {@code @ConditionalOnProperty} cannot be read from
     * {@link com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties}. Keeping the
     * literals here means a key renamed in {@code application.yml} has exactly one place to be renamed
     * in code, rather than being spread across the classes that happen to reference it.
     */
    public static final class ConfigKeys {

        private ConfigKeys() {
        }

        // ------------------------------------------------------------------ prefixes

        public static final String BROADCAST_PREFIX = "broadcast";
        public static final String SIMULATOR_PREFIX = "broadcast.simulator";

        // -------------------------------------------------------------------- topics

        public static final String TOPIC_OUTBOUND_MESSAGES = "${broadcast.topics.outbound-messages}";
        public static final String TOPIC_CAPACITY_UPDATES = "${broadcast.topics.capacity-updates}";

        // --------------------------------------------------------------------- kafka

        public static final String KAFKA_BOOTSTRAP_SERVERS = "${spring.kafka.bootstrap-servers}";
        public static final String KAFKA_CONSUMER_GROUP_ID = "${spring.kafka.consumer.group-id:broadcast-service}";
        public static final String KAFKA_MAX_POLL_RECORDS = "${spring.kafka.consumer.max-poll-records:100}";
        public static final String KAFKA_DISPATCH_CONCURRENCY = "${broadcast.kafka.dispatch-concurrency:6}";

        /** Guards topic auto-creation, which is off outside local development. */
        public static final String KAFKA_AUTO_CREATE_TOPICS = "broadcast.kafka.auto-create-topics";
        public static final String ENABLED_TRUE = "true";

        // ------------------------------------------------------------------ schedules

        public static final String DISPATCH_HOUSEKEEPING_INTERVAL =
                "${broadcast.dispatch.housekeeping-interval:PT60S}";

        // ------------------------------------------- keys quoted in validation messages

        public static final String SIMULATOR_CALLBACK_URL_KEY = "broadcast.simulator.callback-url";
        public static final String SIMULATOR_MIN_DELAY_KEY = "broadcast.simulator.min-delay";
        public static final String SIMULATOR_MAX_DELAY_KEY = "broadcast.simulator.max-delay";
        public static final String SIMULATOR_MAX_CONNECTIONS_KEY = "broadcast.simulator.max-connections";
        public static final String SIMULATOR_MAX_IN_FLIGHT_KEY = "broadcast.simulator.max-in-flight";
    }
}
