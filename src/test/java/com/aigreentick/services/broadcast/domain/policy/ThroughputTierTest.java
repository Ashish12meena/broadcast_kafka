package com.aigreentick.services.broadcast.domain.policy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ThroughputTierTest {

    private static final int DEFAULT_MPS = 80;

    @Test
    void knownTiers() {
        assertThat(ThroughputTier.messagesPerSecond("STANDARD", DEFAULT_MPS)).isEqualTo(80);
        assertThat(ThroughputTier.messagesPerSecond("HIGH_THROUGHPUT", DEFAULT_MPS)).isEqualTo(1_000);
    }

    @Test
    @DisplayName("coexistence numbers are fixed at 20 and must not be treated as standard")
    void coexistenceTier() {
        // Treating one of these as a standard 80 mps number produces continuous rate limiting that
        // no amount of degrade-and-recover can settle, because the ceiling itself is wrong.
        assertThat(ThroughputTier.messagesPerSecond("COEXISTENCE", DEFAULT_MPS)).isEqualTo(20);
    }

    @Test
    @DisplayName("an unrecognised tier falls back rather than throwing")
    void unknownTierFallsBack() {
        // Meta adds tiers. A number that cannot send at all until this class is redeployed is far
        // worse than one sending at a conservative rate.
        assertThat(ThroughputTier.messagesPerSecond("SOMETHING_NEW", DEFAULT_MPS)).isEqualTo(DEFAULT_MPS);
        assertThat(ThroughputTier.messagesPerSecond(null, DEFAULT_MPS)).isEqualTo(DEFAULT_MPS);
        assertThat(ThroughputTier.messagesPerSecond("  ", DEFAULT_MPS)).isEqualTo(DEFAULT_MPS);
    }
}
