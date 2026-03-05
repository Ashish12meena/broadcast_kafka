package com.aigreentick.services.broadcast.client.service;

import com.aigreentick.services.broadcast.client.dto.MessageResultCallbackRequest;

/**
 * Client for reporting message send results back to the messaging service.
 * Called asynchronously after each batch completes — fire-and-forget.
 */
public interface MessagingCallbackClient {

    /**
     * Report the results of a batch of Meta API calls back to the messaging service.
     * This allows the messaging service to update message statuses and recipient states.
     *
     * Fire-and-forget — broadcast service does not wait for the response.
     */
    void reportResults(MessageResultCallbackRequest request);
}