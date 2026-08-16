package com.aigreentick.services.broadcast.application.port.out;

import com.aigreentick.services.broadcast.domain.model.RateGrant;

/**
 * The shared meter. Every send in the platform passes through this.
 *
 * <p>Correctness across instances comes from subtraction, not replication: two instances each
 * wanting 300 against a limit of 500 receive 300 and 200, because they are drawing down the same
 * pool. No instance needs to know how many others exist.
 */
public interface RateLimiterPort {

    /**
     * @param requested how many sends the caller would like
     * @return how many it may perform now, which may be fewer than requested or none
     */
    RateGrant acquire(String phoneNumberId, int requested);
}
