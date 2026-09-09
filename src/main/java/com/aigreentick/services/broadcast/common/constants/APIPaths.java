package com.aigreentick.services.broadcast.common.constants;

/**
 * Every URL, endpoint, path and path fragment this service exposes or calls.
 *
 * <p>Host names and base URLs are deliberately absent: those are environment-specific and arrive
 * through {@code broadcast.meta.base-url} and {@code broadcast.simulator.callback-url}. What lives
 * here is the shape of a path, which is a property of the contract rather than of a deployment.
 */
public final class APIPaths {

    private APIPaths() {
    }

    // ------------------------------------------------------------ internal ops API

    /** Base path for the read-only operational endpoints. */
    public static final String INTERNAL_BROADCAST_BASE = "/internal/broadcast";

    /** Snapshot of what this instance is currently doing. */
    public static final String STATS = "/stats";

    /** Capacity currently believed for one phone number. */
    public static final String CAPACITY_BY_PHONE_NUMBER = "/capacity/{phoneNumberId}";

    // ------------------------------------------------------------- log control API

    /** Active log level overrides and targeted debug scopes on this instance. */
    public static final String LOG_CONTROL = "/logging";

    /** Turn verbose logging on for a single campaign, leaving every other campaign untouched. */
    public static final String LOG_DEBUG_CAMPAIGN = "/logging/campaign/{campaignId}";

    /** As above, scoped to one phone number. */
    public static final String LOG_DEBUG_PHONE_NUMBER = "/logging/phone-number/{phoneNumberId}";

    /** Name of the campaign path variable on the log control endpoints. */
    public static final String PATH_VAR_CAMPAIGN_ID = "campaignId";

    // --------------------------------------------------------------- Meta Cloud API

    /**
     * Send endpoint, relative to {@code broadcast.meta.base-url}.
     *
     * <p>Addressed by Meta's phone number id, never by the platform's own {@code wabaAccountId} —
     * substituting one for the other fails against a number that looks correctly configured.
     */
    public static final String META_SEND_MESSAGE = "/{phoneNumberId}/messages";

    // ------------------------------------------------------------------ path pieces

    /** Name of the phone number path variable, shared by the paths above. */
    public static final String PATH_VAR_PHONE_NUMBER_ID = "phoneNumberId";

    // ------------------------------------------------------------------- auth scheme

    /** Prefix for the {@code Authorization} header value on Meta calls. */
    public static final String BEARER_PREFIX = "Bearer ";
}
