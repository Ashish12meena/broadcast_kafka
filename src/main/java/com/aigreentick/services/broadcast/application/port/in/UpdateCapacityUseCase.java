package com.aigreentick.services.broadcast.application.port.in;

import com.aigreentick.services.broadcast.domain.model.PhoneNumberCapacity;

/** Applies a capacity change published by the Messaging Service. */
public interface UpdateCapacityUseCase {

    void apply(PhoneNumberCapacity capacity);
}
