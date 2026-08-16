package com.aigreentick.services.broadcast.application.port.out;

import com.aigreentick.services.broadcast.domain.model.BatchResult;

/**
 * Reports outcomes back to the Messaging Service.
 *
 * <p>This is the one piece of state the service cannot afford to drop: a lost result leaves a
 * message row stuck mid-flight, and the recovery path for that is to send it to the customer a
 * second time.
 */
public interface ResultPublisherPort {

    void publish(BatchResult result);
}
