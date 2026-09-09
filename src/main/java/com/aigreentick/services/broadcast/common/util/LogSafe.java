package com.aigreentick.services.broadcast.common.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Makes values safe to put in a log line.
 *
 * <h2>Why this is not optional here</h2>
 * A dispatch payload is a list of recipients, and a recipient is a phone number belonging to a real
 * person. Logging one puts it in the aggregator for the whole retention window, replicated across
 * every index and backup — which is personal data processing that nobody consented to and that no
 * retention policy covers. Under the DPDP Act and GDPR the log store is in scope like any other
 * store.
 *
 * <p>The recoverability argument for logging a payload does not hold either: the Kafka coordinates
 * are already in the line, and the source topic still has the bytes. A fingerprint is enough to
 * confirm two occurrences are the same message.
 */
public final class LogSafe {

    /** Enough to see the shape of a malformed message, far short of a recipient list. */
    public static final int DEFAULT_MAX_PAYLOAD_CHARS = 256;

    private static final int FINGERPRINT_CHARS = 12;
    private static final int MSISDN_PREFIX_KEPT = 5;
    private static final int MSISDN_SUFFIX_KEPT = 4;

    private LogSafe() {
    }

    /**
     * Trims a payload to a bounded prefix and appends the original length, so a truncated line still
     * says how much was left out.
     */
    public static String truncate(String raw) {
        return truncate(raw, DEFAULT_MAX_PAYLOAD_CHARS);
    }

    public static String truncate(String raw, int maxChars) {
        if (raw == null) {
            return null;
        }
        if (raw.length() <= maxChars) {
            return raw;
        }
        return raw.substring(0, maxChars) + "...[" + raw.length() + " chars total]";
    }

    /**
     * A stable short digest of a payload. Two log lines carrying the same fingerprint are the same
     * message, which is the only thing the payload was ever actually needed for in a log.
     */
    public static String fingerprint(String raw) {
        if (raw == null) {
            return "null";
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(raw.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest).substring(0, FINGERPRINT_CHARS);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the JLS; unreachable on any conformant JVM.
            return "unavailable";
        }
    }

    /**
     * Masks the subscriber portion of a phone number, keeping country and operator prefix so a log
     * line still tells you which region a problem is concentrated in.
     *
     * <p>{@code +919876543210} becomes {@code +9198****3210}.
     */
    public static String maskPhone(String msisdn) {
        if (msisdn == null || msisdn.isBlank()) {
            return "unknown";
        }
        if (msisdn.length() <= MSISDN_PREFIX_KEPT + MSISDN_SUFFIX_KEPT) {
            return "****";
        }
        return msisdn.substring(0, MSISDN_PREFIX_KEPT)
                + "****"
                + msisdn.substring(msisdn.length() - MSISDN_SUFFIX_KEPT);
    }
}
