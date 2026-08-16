package com.aigreentick.services.broadcast.application.service.ingest;

import com.aigreentick.services.broadcast.application.port.in.DispatchBatchUseCase;
import com.aigreentick.services.broadcast.application.service.dispatch.DispatchScheduler;
import com.aigreentick.services.broadcast.domain.model.DispatchBatch;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * The inbound edge: takes a batch from Kafka and puts it in the right queue.
 *
 * <p>Returns immediately. The caller is a Kafka consumer thread, and anything slow here stops the
 * poll loop for the whole partition.
 *
 * <p>After accepting, it asks the flow controller whether consumption should pause. That check is
 * the reason a slow Meta cannot be absorbed into heap: without it, the consumer keeps fetching and
 * queueing regardless of whether anything downstream is draining, and the only limit on memory is
 * the size of the topic.
 */
@Service
public class BatchIngestService implements DispatchBatchUseCase {

    private static final Logger log = LoggerFactory.getLogger(BatchIngestService.class);

    private final DispatchScheduler scheduler;
    private final ConsumerFlowController flowController;
    private final BroadcastProperties properties;

    public BatchIngestService(
            DispatchScheduler scheduler,
            ConsumerFlowController flowController,
            BroadcastProperties properties) {
        this.scheduler = scheduler;
        this.flowController = flowController;
        this.properties = properties;
    }

    @Override
    public void accept(DispatchBatch batch, Runnable onComplete) {
        if (batch.size() == 0) {
            // Nothing to send, but the offset still has to move or the partition stalls here forever.
            log.warn("Empty batch campaignId={} phoneNumberId={}; acknowledging",
                    batch.campaignId(), batch.phoneNumberId());
            onComplete.run();
            return;
        }

        InFlightBatch inFlight = new InFlightBatch(batch, onComplete);
        scheduler.enqueue(inFlight);

        log.info("Batch accepted campaignId={} phoneNumberId={} recipients={}",
                batch.campaignId(), batch.phoneNumberId(), batch.size());

        int deepest = scheduler.deepestQueue();
        if (deepest >= properties.dispatch().maxQueuedBatchesPerNumber()) {
            flowController.pauseIfRunning(
                    "queue depth %d reached the limit of %d"
                            .formatted(deepest, properties.dispatch().maxQueuedBatchesPerNumber()));
        }
    }
}
