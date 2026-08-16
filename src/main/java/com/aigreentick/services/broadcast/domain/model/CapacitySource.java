package com.aigreentick.services.broadcast.domain.model;

/**
 * Where a phone number's current capacity figure came from.
 *
 * <p>Exported as a metric because the difference matters operationally: running on
 * {@link #LOCAL_FALLBACK} means Redis is unreachable and the global limit is no longer being
 * enforced across instances, which is a condition that must be visible rather than merely logged.
 */
public enum CapacitySource {

    /** Published by the Messaging Service and read from Redis. The normal case. */
    CAPACITY_EVENT,

    /** Reduced by this service after Meta reported a rate limit. */
    DEGRADED,

    /** Redis is unreachable; a conservative local estimate is in use. */
    LOCAL_FALLBACK,

    /** No capacity is known for this number; the configured default applies. */
    DEFAULT
}
