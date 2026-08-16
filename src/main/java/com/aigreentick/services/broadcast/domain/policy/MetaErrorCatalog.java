package com.aigreentick.services.broadcast.domain.policy;

import java.util.Set;

/**
 * Maps Meta Cloud API error codes onto {@link MetaErrorClass}.
 *
 * <p>Anything unrecognised is treated as {@link MetaErrorClass#PERMANENT}. That is the conservative
 * direction: a message wrongly given up on is one undelivered message with a recorded reason, while
 * a message wrongly retried forever is a loop that can send the same customer the same thing
 * repeatedly.
 *
 * <p>This must stay identical to the Messaging Service's catalog. If the two drift, one service
 * retries what the other abandons and they disagree about what a number's rate should be — which is
 * why the companion design document proposes promoting this to a shared module rather than
 * maintaining two copies.
 *
 * <p>Codes are from Meta's Cloud API error reference. Worth re-checking when the Graph version moves.
 */
public final class MetaErrorCatalog {

    /** Over the throughput limit. The response is to slow the phone number down. */
    private static final Set<Integer> RATE_LIMIT = Set.of(
            4,      // application-level rate limit hit
            80007,  // per-WABA rate limit hit
            130429, // Cloud API throughput limit for this number
            131048  // spam rate limit — sending too fast for the number's quality rating
    );

    /** Too many messages to one WhatsApp user too quickly. */
    private static final int PAIR_RATE_LIMIT = 131056;

    /** Throughput upgrade in progress; Meta documents this as lasting up to about a minute. */
    private static final int UPGRADE_IN_PROGRESS = 131057;

    /** Transient conditions unrelated to the content of the message. */
    private static final Set<Integer> TRANSIENT = Set.of(
            1,      // unknown API error, documented as usually transient
            2,      // temporary Graph API service failure
            133016  // account temporarily locked while a registration completes
    );

    /** The token was rejected. Not the message's fault. */
    private static final Set<Integer> CREDENTIAL = Set.of(
            190,    // token expired or revoked
            0,      // cannot parse access token
            10      // permission denied for this token's scopes
    );

    private MetaErrorCatalog() {
    }

    public static MetaErrorClass classify(Integer code) {
        if (code == null) {
            return MetaErrorClass.PERMANENT;
        }
        if (RATE_LIMIT.contains(code)) {
            return MetaErrorClass.RATE_LIMIT;
        }
        if (code == PAIR_RATE_LIMIT) {
            return MetaErrorClass.PAIR_RATE_LIMIT;
        }
        if (code == UPGRADE_IN_PROGRESS) {
            return MetaErrorClass.UPGRADE_IN_PROGRESS;
        }
        if (TRANSIENT.contains(code)) {
            return MetaErrorClass.TRANSIENT;
        }
        if (CREDENTIAL.contains(code)) {
            return MetaErrorClass.CREDENTIAL;
        }
        return MetaErrorClass.PERMANENT;
    }

    /** Whether another attempt could plausibly succeed with the same payload. */
    public static boolean retryable(MetaErrorClass errorClass) {
        return switch (errorClass) {
            case RATE_LIMIT, PAIR_RATE_LIMIT, UPGRADE_IN_PROGRESS, TRANSIENT -> true;
            case CREDENTIAL, PERMANENT -> false;
        };
    }
}
