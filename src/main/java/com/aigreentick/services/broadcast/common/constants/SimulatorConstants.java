package com.aigreentick.services.broadcast.common.constants;

import java.time.Duration;

/**
 * Defaults and literals for the test-profile Meta simulator.
 *
 * <p>Separate from {@link DomainConstants.Meta} because none of it describes Meta's contract — it
 * describes how convincingly this service pretends to be Meta. The defaults here are the values
 * applied when {@code broadcast.simulator.*} leaves them unset.
 */
public final class SimulatorConstants {

    private SimulatorConstants() {
    }

    /** Name of the simulator's own connection pool, kept separate from the Meta pool. */
    public static final String CALLBACK_POOL_NAME = "simulator-callback";

    /** Bean name of the simulator's WebClient. */
    public static final String CALLBACK_WEB_CLIENT = "simulatorCallbackWebClient";

    // ----------------------------------------------------------------- delay defaults

    /**
     * Randomness is the point — it makes messages overtake each other and exercises the receiver's
     * out-of-order handling — but the magnitude buys nothing except waiting.
     */
    public static final Duration DEFAULT_MIN_DELAY = Duration.ofMillis(200);
    public static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(2);

    // ---------------------------------------------------------------- volume defaults

    /**
     * Raised together on purpose. 128 concurrent posts against a receiver answering in tens of
     * milliseconds clears the ~240 callbacks per second implied by Meta's standard 80 mps tier.
     */
    public static final int DEFAULT_MAX_CONNECTIONS = 128;
    public static final int DEFAULT_MAX_IN_FLIGHT = 128;

    public static final Duration DEFAULT_RESPONSE_TIMEOUT = Duration.ofSeconds(10);
    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(3);

    // ---------------------------------------------------------------- pool behaviour

    /**
     * Generous relative to max-in-flight, which is the real limit. This only needs to be large
     * enough that a brief burst queues instead of being rejected.
     */
    public static final int PENDING_ACQUIRE_MULTIPLIER = 20;

    public static final Duration PENDING_ACQUIRE_TIMEOUT = Duration.ofSeconds(10);
    public static final Duration MAX_IDLE_TIME = Duration.ofSeconds(30);

    // -------------------------------------------------------------------- pipeline

    /** Bounded so overflow is a visible rejected emission rather than an unbounded heap. */
    public static final int PENDING_CALLBACK_BUFFER = 4096;

    /** How often dropped-callback counts are summarised. Frequent enough to notice mid-broadcast. */
    public static final Duration DROP_REPORT_INTERVAL = Duration.ofSeconds(10);

    // -------------------------------------------------------------------- identity

    /**
     * Marks a simulated message id. Simulated ids land in the same tables and topics as real ones,
     * and an id that cannot be told apart makes a polluted test database impossible to clean up.
     */
    public static final String SIMULATED_WAMID_PREFIX = "wamid.SIM.";

    /** Bytes of randomness behind a simulated wamid. */
    public static final int WAMID_RANDOM_BYTES = 24;

    /** Stands in for a recipient number the payload did not carry. */
    public static final String UNKNOWN_RECIPIENT = "0000000000";

    /** Meta reports recipients in international format with no plus sign. */
    public static final String NON_DIGIT_PATTERN = "[^0-9]";
}
