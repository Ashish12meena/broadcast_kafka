package com.aigreentick.services.broadcast.application.port.in;

import com.aigreentick.services.broadcast.domain.model.DispatchBatch;

/**
 * Accepts a batch for transmission. The inbound side of the service.
 *
 * <p>Implementations must return promptly — the caller is a Kafka consumer thread and blocking it
 * stops the poll loop.
 *
 * @see com.aigreentick.services.broadcast.application.service.ingest.BatchIngestService
 */
public interface DispatchBatchUseCase {

    /**
     * @param batch      the work to transmit
     * @param onComplete run once every recipient in the batch has a recorded outcome; this is where
     *                   the Kafka offset is acknowledged, so it must not run earlier
     */
    void accept(DispatchBatch batch, Runnable onComplete);
}
