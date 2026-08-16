package com.aigreentick.services.broadcast.domain.policy;

/**
 * How this service should react to a Meta error code.
 *
 * <p>The classification exists because Meta's HTTP status cannot answer the question: almost
 * everything arrives as 400, and "you are sending too fast, wait a moment" is distinguished from
 * "this number is not on WhatsApp and never will be" only by the numeric code in the body.
 */
public enum MetaErrorClass {

    /** The number is over its throughput limit. Slow the number down and try again behind the meter. */
    RATE_LIMIT,

    /**
     * Too many messages to one recipient too quickly. A property of the pair, not of the number —
     * degrading the whole phone number over this would throttle thousands of unrelated recipients.
     */
    PAIR_RATE_LIMIT,

    /**
     * The number is mid throughput upgrade and briefly unusable. Not a failure and not a reason to
     * reduce capacity: this number is about to get faster, not slower.
     */
    UPGRADE_IN_PROGRESS,

    /** Capacity or timing, not the message. The same payload can succeed unchanged. */
    TRANSIENT,

    /** The access token was rejected. Retrying the payload cannot help. */
    CREDENTIAL,

    /** Something about this message will never work. Record the reason and move on. */
    PERMANENT
}
