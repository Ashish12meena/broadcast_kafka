package com.aigreentick.services.broadcast.application.port.out;

import com.aigreentick.services.broadcast.domain.model.PhoneNumberCapacity;

import java.util.Optional;

/** Where a phone number's current capacity is kept so that every instance sees the same figure. */
public interface CapacityStorePort {

    Optional<PhoneNumberCapacity> find(String phoneNumberId);

    void put(PhoneNumberCapacity capacity);

    /** Reduces the working rate and suppresses the number until {@code backoffUntilMs}. */
    void degrade(String phoneNumberId, int newEffectiveMps, long backoffUntilMs);

    /**
     * Admits one degrade per phone number per short window.
     *
     * <p>A burst of four hundred concurrent rate-limit responses describes one condition, not four
     * hundred. Without this, halving on each would drive the number's rate to one message per second
     * and leave it there.
     *
     * @return true if the caller holds the lock and should perform the degrade
     */
    boolean tryAcquireDegradeLock(String phoneNumberId);
}
