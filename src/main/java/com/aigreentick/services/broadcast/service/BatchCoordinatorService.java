package com.aigreentick.services.broadcast.service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class BatchCoordinatorService {

    private final WhatsappClient whatsappClient;
    private final ObjectMapper objectMapper;
    private final ExecutorService whatsappExecutor;
    private final ConcurrentHashMap<String, Semaphore> userSemaphores;
    private final ConcurrentHashMap<String, Long> semaphoreLastUsed;

    @Value("${batch.size:80}")
    private int batchSize;

    // Per-user queues (lightweight, no threads)
    private final ConcurrentHashMap<String, UserQueue> userQueues = new ConcurrentHashMap<>();

    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    // Metrics
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong totalBatches = new AtomicLong(0);

    public BatchCoordinator(
            WhatsappClient whatsappClient,
            ReportServiceImpl reportService,
            ObjectMapper objectMapper,
            @Qualifier("whatsappExecutor") ExecutorService whatsappExecutor,
            ConcurrentHashMap<String, Semaphore> userSemaphores,
            ConcurrentHashMap<String, Long> semaphoreLastUsed) {
        this.whatsappClient = whatsappClient;
        this.reportService = reportService;
        this.objectMapper = objectMapper;
        this.whatsappExecutor = whatsappExecutor;
        this.userSemaphores = userSemaphores;
        this.semaphoreLastUsed = semaphoreLastUsed;
    }

    /**
     * Add event to user's queue. Never blocks.
     * Submits processing task if not already running.
     */
    public void addEventToBatch(BroadcastReportEvent event, Acknowledgment acknowledgment) {

        if (shutdownRequested.get()) {
            log.warn("Shutdown requested, acknowledging without processing");
            acknowledgment.acknowledge();
            return;
        }

        String phoneNumberId = event.getPhoneNumberId();

        // Get or create user queue
        UserQueue userQueue = userQueues.computeIfAbsent(
                phoneNumberId,
                k -> new UserQueue(phoneNumberId));

        // Add item to queue (non-blocking)
        userQueue.addItem(new BatchItem(event, acknowledgment));

        // Try to start processing if not already running
        if (userQueue.tryStartProcessing()) {
            submitProcessingTask(userQueue);
        }
    }

    /**
     * Submit processing task to thread pool
     */
    private void submitProcessingTask(UserQueue userQueue) {
        whatsappExecutor.submit(() -> processUserQueue(userQueue));
    }

    /**
     * Process all pending items for a user.
     * Runs on pooled thread, exits when queue is empty.
     */
    private void processUserQueue(UserQueue userQueue) {
        String phoneNumberId = userQueue.getPhoneNumberId();

        log.debug("Processing task started for phoneNumberId={}", phoneNumberId);

        try {
            while (!shutdownRequested.get()) {
                // Collect batch from queue
                List<BatchItem> batch = collectBatch(userQueue);

                if (batch.isEmpty()) {
                    // Queue is empty, try to exit
                    if (userQueue.tryStopProcessing()) {
                        // Successfully marked as not processing
                        // Final check: if queue still empty, exit
                        if (userQueue.isEmpty()) {
                            log.debug("Processing task exiting for phoneNumberId={} (queue empty)",
                                    phoneNumberId);
                            return;
                        } else {
                            // Race condition: new items arrived
                            // Restart processing
                            if (userQueue.tryStartProcessing()) {
                                log.debug("Restarting processing for phoneNumberId={} (new items arrived)",
                                        phoneNumberId);
                                continue;
                            } else {
                                // Another task started, we can exit
                                return;
                            }
                        }
                    } else {
                        // Someone else is handling it
                        return;
                    }
                }

                // Process the batch
                processBatch(phoneNumberId, batch);
            }
        } catch (Exception e) {
            log.error("Processing task failed for phoneNumberId={}", phoneNumberId, e);
        } finally {
            // Ensure processing flag is cleared on unexpected exit
            userQueue.forceStopProcessing();
        }
    }

    /**
     * Collect items from queue into a batch (up to batchSize)
     */
    private List<BatchItem> collectBatch(UserQueue userQueue) {
        List<BatchItem> batch = new ArrayList<>();

        BatchItem item;
        while (batch.size() < batchSize && (item = userQueue.poll()) != null) {
            batch.add(item);
        }

        return batch;
    }

    /**
     * Process a batch of items
     */
    private void processBatch(String phoneNumberId, List<BatchItem> batch) {
        long startTime = System.currentTimeMillis();

        log.info("=== Processing Batch ===");
        log.info("PhoneNumberId: {} | Size: {} | Pending: {}",
                phoneNumberId, batch.size(), userQueues.get(phoneNumberId).size());

        try {
            // STAGE 1: WhatsApp API Calls
            List<WhatsAppResult> results = sendWhatsAppBatch(phoneNumberId, batch);

            // STAGE 2: Acknowledge Kafka messages
            acknowledgeAllMessages(batch);


            // STAGE 3: Database Update
            batchUpdateDatabase(batch, results);


            // Update metrics
            totalProcessed.addAndGet(batch.size());
            totalBatches.incrementAndGet();

            long duration = System.currentTimeMillis() - startTime;
            log.info("=== Batch Completed ===");
            log.info("PhoneNumberId: {} | Items: {} | Duration: {}ms",
                    phoneNumberId, batch.size(), duration);

        } catch (Exception e) {
            log.error("Batch processing failed for phoneNumberId={}", phoneNumberId, e);
            handleBatchFailure(batch, e);
        }
    }

    /**
     * STAGE 1: Send WhatsApp requests concurrently
     */
    private List<WhatsAppResult> sendWhatsAppBatch(String phoneNumberId, List<BatchItem> batch) {
        long stageStart = System.currentTimeMillis();

        Semaphore userSemaphore = ExecutorConfig.getSemaphoreForUser(
                userSemaphores, semaphoreLastUsed, phoneNumberId);

        List<Semaphore> acquiredSemaphores = new ArrayList<>();
        List<CompletableFuture<WhatsAppResult>> futures = new ArrayList<>();

        try {
            log.debug("Stage 1: Acquiring {} permits", batch.size());

            // Acquire permits
            for (int i = 0; i < batch.size(); i++) {
                userSemaphore.acquire();
                acquiredSemaphores.add(userSemaphore);
            }

            log.debug("Stage 1: Permits acquired. Available: {}",
                    userSemaphore.availablePermits());

            // Submit concurrent WhatsApp requests using the executor
            for (BatchItem item : batch) {
                CompletableFuture<WhatsAppResult> future = CompletableFuture
                        .supplyAsync(() -> sendSingleWhatsAppMessage(item.event()), whatsappExecutor);
                futures.add(future);
            }

            // Wait for all responses
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(300, TimeUnit.SECONDS);

            // Collect results
            List<WhatsAppResult> results = new ArrayList<>();
            for (CompletableFuture<WhatsAppResult> future : futures) {
                results.add(future.get());
            }

            long stageDuration = System.currentTimeMillis() - stageStart;
            log.info("Stage 1: Completed. Duration: {}ms | Success: {}/{}",
                    stageDuration,
                    results.stream().filter(WhatsAppResult::success).count(),
                    results.size());

            return results;

        } catch (Exception e) {
            log.error("Stage 1: Failed for phoneNumberId={}", phoneNumberId, e);

            // Create error results
            List<WhatsAppResult> errorResults = new ArrayList<>();
            for (BatchItem item : batch) {
                errorResults.add(new WhatsAppResult(
                        item.event().getBroadcastId(),
                        item.event().getRecipient(),
                        null,
                        false,
                        "Batch error: " + e.getMessage()));
            }
            return errorResults;

        } finally {
            // Release all permits
            for (Semaphore sem : acquiredSemaphores) {
                sem.release();
            }
            log.debug("Stage 1: Released {} permits", acquiredSemaphores.size());
        }
    }

    /**
     * Send single WhatsApp message
     */
    private WhatsAppResult sendSingleWhatsAppMessage(BroadcastReportEvent event) {
        try {
            FacebookApiResponse<SendTemplateMessageResponse> response = whatsappClient.sendMessage(
                    event.getPayload(),
                    event.getPhoneNumberId(),
                    event.getAccessToken());

            return new WhatsAppResult(
                    event.getBroadcastId(),
                    event.getRecipient(),
                    response,
                    response.isSuccess(),
                    null);

        } catch (Exception e) {
            log.error("WhatsApp request failed: recipient={}", event.getRecipient(), e);
            return new WhatsAppResult(
                    event.getBroadcastId(),
                    event.getRecipient(),
                    null,
                    false,
                    e.getMessage());
        }
    }

    /**
     * STAGE 2: Batch database update
     */
    private void batchUpdateDatabase(List<BatchItem> batch, List<WhatsAppResult> results) {
        long stageStart = System.currentTimeMillis();

        List<DatabaseUpdate> updates = new ArrayList<>();

        for (int i = 0; i < batch.size(); i++) {
            BatchItem item = batch.get(i);
            WhatsAppResult result = results.get(i);

            try {
                String responseJson = objectMapper.writeValueAsString(result.response());
                String status;
                String messageStatusValue;
                String whatsappMessageId = null;

                if (result.success() && result.response() != null) {
                    status = "sent";
                    SendTemplateMessageResponse data = result.response().getData();
                    if (data != null && data.getMessages() != null &&
                            !data.getMessages().isEmpty()) {
                        var msg = data.getMessages().get(0);
                        whatsappMessageId = msg.getId();
                        messageStatusValue = msg.getMessageStatus() != null ? msg.getMessageStatus() : "sent";
                    } else {
                        messageStatusValue = "sent";
                    }
                } else {
                    status = "failed";
                    messageStatusValue = result.errorMessage() != null ? result.errorMessage() : "Failed";
                }

                updates.add(new DatabaseUpdate(
                        item.event().getBroadcastId(),
                        item.event().getRecipient(),
                        responseJson,
                        status,
                        messageStatusValue,
                        whatsappMessageId,
                        item.event().getPayload(),
                        LocalDateTime.now()));

            } catch (Exception e) {
                log.error("Failed to prepare update: recipient={}",
                        item.event().getRecipient(), e);
            }
        }

        try {
            int successCount = reportService.batchUpdateReports(updates);

            long stageDuration = System.currentTimeMillis() - stageStart;
            log.info("Stage 2: Completed. Duration: {}ms | Updated: {}/{}",
                    stageDuration, successCount, updates.size());

        } catch (Exception e) {
            log.error("Stage 2: Failed", e);
            throw e;
        }
    }

    /**
     * STAGE 3: Acknowledge Kafka messages
     */
    private void acknowledgeAllMessages(List<BatchItem> batch) {
        for (BatchItem item : batch) {
            try {
                item.acknowledgment().acknowledge();
            } catch (Exception e) {
                log.error("Failed to acknowledge: recipient={}",
                        item.event().getRecipient(), e);
            }
        }
    }

    /**
     * Handle batch failure
     */
    private void handleBatchFailure(List<BatchItem> batch, Exception error) {
        log.error("Handling batch failure for {} items", batch.size());

        for (BatchItem item : batch) {
            try {
                item.acknowledgment().acknowledge();
            } catch (Exception e) {
                log.error("Failed to acknowledge failed message", e);
            }
        }
    }

    /**
     * Cleanup empty queues periodically
     */
    @Scheduled(fixedRate = 300000) // Every 5 minutes
    public void cleanupEmptyQueues() {
        int removed = 0;

        for (var entry : userQueues.entrySet()) {
            UserQueue queue = entry.getValue();

            // Only remove if empty and not processing
            if (queue.isEmpty() && !queue.isProcessing()) {
                if (userQueues.remove(entry.getKey(), queue)) {
                    removed++;
                }
            }
        }

        if (removed > 0) {
            log.info("Cleaned up {} empty user queues. Remaining: {}", removed, userQueues.size());
        }
    }

    /**
     * Graceful shutdown
     */
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down BatchCoordinator...");
        shutdownRequested.set(true);

        // Process remaining items in all queues
        for (UserQueue queue : userQueues.values()) {
            List<BatchItem> remaining = new ArrayList<>();
            BatchItem item;
            while ((item = queue.poll()) != null) {
                remaining.add(item);
            }

            if (!remaining.isEmpty()) {
                log.info("Processing {} remaining items for phoneNumberId={}",
                        remaining.size(), queue.getPhoneNumberId());
                processBatch(queue.getPhoneNumberId(), remaining);
            }
        }

        log.info("BatchCoordinator shutdown complete. Total processed: {}, Total batches: {}",
                totalProcessed.get(), totalBatches.get());
    }

    /**
     * Get statistics
     */
    public BatchStats getStats() {
        int activeQueues = 0;
        int processingQueues = 0;
        int totalPending = 0;

        for (UserQueue queue : userQueues.values()) {
            if (!queue.isEmpty()) {
                activeQueues++;
                totalPending += queue.size();
            }
            if (queue.isProcessing()) {
                processingQueues++;
            }
        }

        return new BatchStats(
                userQueues.size(),
                activeQueues,
                processingQueues,
                totalPending,
                totalProcessed.get(),
                totalBatches.get());
    }

    // ==================== INNER CLASSES ====================

    /**
     * Per-user queue wrapper with processing state
     */
    private static class UserQueue {
        private final String phoneNumberId;
        private final ConcurrentLinkedQueue<BatchItem> queue;
        private final AtomicBoolean processing;
        private final AtomicLong lastActivity;

        public UserQueue(String phoneNumberId) {
            this.phoneNumberId = phoneNumberId;
            this.queue = new ConcurrentLinkedQueue<>();
            this.processing = new AtomicBoolean(false);
            this.lastActivity = new AtomicLong(System.currentTimeMillis());
        }

        public String getPhoneNumberId() {
            return phoneNumberId;
        }

        public void addItem(BatchItem item) {
            queue.offer(item);
            lastActivity.set(System.currentTimeMillis());
        }

        public BatchItem poll() {
            return queue.poll();
        }

        public boolean isEmpty() {
            return queue.isEmpty();
        }

        public int size() {
            return queue.size();
        }

        public boolean isProcessing() {
            return processing.get();
        }

        /**
         * Try to start processing. Returns true if we should start.
         */
        public boolean tryStartProcessing() {
            return processing.compareAndSet(false, true);
        }

        /**
         * Try to stop processing. Returns true if we stopped.
         */
        public boolean tryStopProcessing() {
            return processing.compareAndSet(true, false);
        }

        /**
         * Force stop processing (for error cases)
         */
        public void forceStopProcessing() {
            processing.set(false);
        }

        public long getLastActivity() {
            return lastActivity.get();
        }
    }

    public record BatchItem(
            BroadcastReportEvent event,
            Acknowledgment acknowledgment) {
    }

    private record WhatsAppResult(
            Long broadcastId,
            String recipient,
            FacebookApiResponse<SendTemplateMessageResponse> response,
            boolean success,
            String errorMessage) {
    }

    public record DatabaseUpdate(
            Long broadcastId,
            String mobile,
            String responseJson,
            String status,
            String messageStatus,
            String whatsappMessageId,
            String payload,
            LocalDateTime timestamp) {
    }

    public record BatchStats(
            int totalQueues,
            int activeQueues,
            int processingQueues,
            int totalPending,
            long totalProcessed,
            long totalBatches) {
    }
}
