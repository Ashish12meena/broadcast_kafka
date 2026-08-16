package com.aigreentick.services.broadcast.domain.policy;

/**
 * Translates Meta's throughput tier name into messages per second.
 *
 * <p>The rate for each tier is Meta's published figure, identical for every tenant, changing only
 * when Meta changes it — a fact about the provider rather than a per-number setting.
 *
 * <p>An unrecognised tier returns the configured default rather than throwing. Meta adds tiers, and
 * a phone number that cannot send at all until this class is redeployed is a far worse outcome than
 * one sending at a conservative rate.
 */
public final class ThroughputTier {

    /** Meta's default for a registered business phone number. */
    public static final int STANDARD_MPS = 80;

    /** Meta's higher tier, granted by automatic upgrade. */
    public static final int HIGH_THROUGHPUT_MPS = 1_000;

    /**
     * Fixed rate for numbers in use with both the WhatsApp Business app and Cloud API. Meta does not
     * upgrade these, so treating one as a standard number guarantees continuous rate limiting.
     */
    public static final int COEXISTENCE_MPS = 20;

    private ThroughputTier() {
    }

    public static int messagesPerSecond(String tier, int defaultMps) {
        if (tier == null || tier.isBlank()) {
            return defaultMps;
        }
        return switch (tier.trim().toUpperCase()) {
            case "STANDARD" -> STANDARD_MPS;
            case "HIGH_THROUGHPUT", "HIGH-THROUGHPUT", "HIGH" -> HIGH_THROUGHPUT_MPS;
            case "COEXISTENCE", "CO_EXISTENCE" -> COEXISTENCE_MPS;
            default -> defaultMps;
        };
    }
}
