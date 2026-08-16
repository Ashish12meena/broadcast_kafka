package com.aigreentick.services.broadcast.domain.policy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MetaErrorCatalogTest {

    @Test
    @DisplayName("throughput codes slow the number down")
    void rateLimitCodes() {
        assertThat(MetaErrorCatalog.classify(130429)).isEqualTo(MetaErrorClass.RATE_LIMIT);
        assertThat(MetaErrorCatalog.classify(80007)).isEqualTo(MetaErrorClass.RATE_LIMIT);
        assertThat(MetaErrorCatalog.classify(4)).isEqualTo(MetaErrorClass.RATE_LIMIT);
    }

    @Test
    @DisplayName("a pair rate limit is about one recipient, not the phone number")
    void pairRateLimitIsSeparate() {
        // Kept distinct so a single chatty recipient cannot throttle every other recipient on the
        // same number.
        assertThat(MetaErrorCatalog.classify(131056)).isEqualTo(MetaErrorClass.PAIR_RATE_LIMIT);
    }

    @Test
    @DisplayName("an upgrade in progress is not a degradation")
    void upgradeIsNotAFailure() {
        assertThat(MetaErrorCatalog.classify(131057)).isEqualTo(MetaErrorClass.UPGRADE_IN_PROGRESS);
    }

    @Test
    @DisplayName("unknown codes are permanent, which is the safe direction")
    void unknownCodesArePermanent() {
        // A message wrongly abandoned is one undelivered message with a reason. A message wrongly
        // retried forever can send the same customer the same thing repeatedly.
        assertThat(MetaErrorCatalog.classify(999999)).isEqualTo(MetaErrorClass.PERMANENT);
        assertThat(MetaErrorCatalog.classify(null)).isEqualTo(MetaErrorClass.PERMANENT);
    }

    @Test
    @DisplayName("credential failures are not retried with the same payload")
    void credentialFailures() {
        assertThat(MetaErrorCatalog.classify(190)).isEqualTo(MetaErrorClass.CREDENTIAL);
        assertThat(MetaErrorCatalog.retryable(MetaErrorClass.CREDENTIAL)).isFalse();
    }
}
