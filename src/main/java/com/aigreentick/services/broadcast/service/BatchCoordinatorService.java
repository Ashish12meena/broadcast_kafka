package com.aigreentick.services.broadcast.service;

import com.aigreentick.services.broadcast.client.dto.MessageResultCallbackRequest;
import com.aigreentick.services.broadcast.client.dto.MetaApiResponse;
import com.aigreentick.services.broadcast.client.dto.WabaCredentials;
import com.aigreentick.services.broadcast.client.service.MessagingCallbackClient;
import com.aigreentick.services.broadcast.client.service.WhatsappClient;
import com.aigreentick.services.broadcast.config.ConfigConstants;
import com.aigreentick.services.broadcast.config.ExecutorConfig;
import com.aigreentick.services.broadcast.kafka.event.BroadcastMessageEvent;
import com.aigreentick.services.broadcast.kafka.event.BroadcastMessageEvent.RecipientPayload;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinates batch processing of broadcast messages.
 *
 * Flow per Kafka message (which is already a batch from messaging service):
 *   1. Consumer calls addBatch() — returns immediately
 *   2. A worker thread picks up the batch from the per-WABA queue
 *   3. Worker resolves WABA credentials (phone_number_id + access_token)
 *   4. Worker sends all recipients to Meta concurrently (bounded by semaphore)
 *   5. Kafka message is acknowledged
 *   6. Results are reported async to messaging service via HTTP callback
 *
 * Semaphore keyed by wabaAccountId to enforce Meta's per-WABA concurrency limits.
 */
@Slf4j
@Service
public class BatchCoordinatorService {

    private final WhatsappClient whatsappClient;
    private final MessagingCallbackClient callbackClient;
    private final WabaCredentialResolver credentialResolver;
    private final ExecutorService whatsappExecutor;
    private final ConcurrentHashMap<String, Semaphore> userSemaphores;
    private final ConcurrentHashMap<String, Long> semaphoreLastUsed;

    // Per-WABA queues (lightweight, no dedicated threads)
    private final ConcurrentHashMap<String, WabaQueue> wabaQueues = new ConcurrentHashMap<>();

    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    // Metrics
    private final AtomicLong totalRecipientsProcessed = new AtomicLong(0);
    private final AtomicLong totalBatchesProcessed = new AtomicLong(0);

    public BatchCoordinatorService(
            WhatsappClient whatsappClient,
            MessagingCallbackClient callbackClient,
            WabaCredentialResolver credentialResolver,
            @Qualifier("whatsappExecutor") ExecutorService whatsappExecutor,
            ConcurrentHashMap<String, Semaphore> userSemaphores,
            ConcurrentHashMap<String, Long> semaphoreLastUsed) {
        this.whatsappClient = whatsappClient;
        this.callbackClient = callbackClient;
        this.credentialResolver = credentialResolver;
        this.whatsappExecutor = whatsappExecutor;
        this.userSemaphores = userSemaphores;
        this.semaphoreLastUsed = semaphoreLastUsed;
    }

    /**
     * Entry point from Kafka consumer. Returns immediately — never blocks the consumer thread.
     */
    public void addBatch(BroadcastMessageEvent event, Acknowledgment acknowledgment) {
        if (shutdownRequested.get()) {
            log.warn("Shutdown requested — acknowledging without processing: campaignId={}", event.getCampaignId());
            acknowledgment.acknowledge();
            return;
        }

        String wabaKey = String.valueOf(event.getWabaAccountId());

        WabaQueue queue = wabaQueues.computeIfAbsent(wabaKey, k -> new WabaQueue(wabaKey));
        queue.enqueue(new QueuedBatch(event, acknowledgment));

        // Try to start a processing task if one isn't already running for this WABA
        if (queue.tryStartProcessing()) {
            whatsappExecutor.submit(() -> drainQueue(queue));
        }
    }

    /**
     * Worker loop: drains all pending batches for a single WABA account.
     * Runs on whatsappExecutor thread pool.
     */
    private void drainQueue(WabaQueue queue) {
        String wabaKey = queue.getWabaKey();
        log.debug("Worker started for wabaKey={}", wabaKey);

        try {
            while (!shutdownRequested.get()) {
                QueuedBatch batch = queue.poll();

                if (batch == null) {
                    // Queue appears empty — try to exit cleanly
                    if (queue.tryStopProcessing()) {
                        if (queue.isEmpty()) {
                            log.debug("Worker exiting for wabaKey={} — queue empty", wabaKey);
                            return;
                        }
                        // Race: new item arrived between isEmpty() check and tryStop
                        if (queue.tryStartProcessing()) {
                            continue; // restart loop
                        }
                        return; // another worker took over
                    }
                    return; // another worker is handling it
                }

                processBatch(wabaKey, batch);
            }
        } catch (Exception e) {
            log.error("Worker failed unexpectedly for wabaKey={}", wabaKey, e);
        } finally {
            queue.forceStopProcessing();
        }
    }

    /**
     * Process a single Kafka batch (one BroadcastMessageEvent = one Kafka message).
     */
    private void processBatch(String wabaKey, QueuedBatch queued) {
        BroadcastMessageEvent event = queued.event();
        long campaignId = event.getCampaignId();
        List<RecipientPayload> recipients = event.getPayloads();
        long batchStart = System.currentTimeMillis();

        log.info("Processing batch: campaignId={} wabaAccountId={} recipients={}",
                campaignId, event.getWabaAccountId(), recipients.size());

        try {
            // Step 1: Resolve WABA credentials (phone_number_id + access_token)
            WabaCredentials credentials = credentialResolver.resolve(event.getWabaAccountId());

            // Step 2: Send all recipients to Meta concurrently
            List<RecipientResult> results = sendToMeta(wabaKey, credentials, recipients, campaignId);

            // Step 3: Acknowledge Kafka — message has been processed
            queued.acknowledgment().acknowledge();
            log.debug("Kafka acknowledged: campaignId={}", campaignId);

            // Step 4: Report results async to messaging service (fire-and-forget)
            reportResultsAsync(event, results);

            // Update metrics
            totalRecipientsProcessed.addAndGet(recipients.size());
            totalBatchesProcessed.incrementAndGet();

            log.info("Batch completed: campaignId={} duration={}ms success={}/{}",
                    campaignId,
                    System.currentTimeMillis() - batchStart,
                    results.stream().filter(RecipientResult::success).count(),
                    results.size());

        } catch (Exception e) {
            log.error("Batch processing failed: campaignId={} error={}", campaignId, e.getMessage(), e);

            // Acknowledge to prevent infinite retry on a persistent error
            // Messaging service will detect missing callbacks and handle recovery
            try {
                queued.acknowledgment().acknowledge();
            } catch (Exception ackEx) {
                log.error("Failed to acknowledge failed batch: campaignId={}", campaignId, ackEx);
            }
        }
    }

    /**
     * Send all recipients in a batch to Meta's WhatsApp API concurrently.
     * Bounded by semaphore to respect Meta's per-WABA concurrency limits.
     */
    private List<RecipientResult> sendToMeta(
            String wabaKey,
            WabaCredentials credentials,
            List<RecipientPayload> recipients,
            long campaignId) {

        Semaphore semaphore = ExecutorConfig.getSemaphoreForUser(userSemaphores, semaphoreLastUsed, wabaKey);
        List<CompletableFuture<RecipientResult>> futures = new ArrayList<>(recipients.size());
        int acquiredPermits = 0;

        try {
            // Acquire permits upfront for the whole batch
            for (int i = 0; i < recipients.size(); i++) {
                semaphore.acquire();
                acquiredPermits++;
            }

            log.debug("Acquired {} permits for wabaKey={} | available={}",
                    acquiredPermits, wabaKey, semaphore.availablePermits());

            // Submit concurrent Meta API calls
            for (RecipientPayload recipient : recipients) {
                final int idx = futures.size();
                CompletableFuture<RecipientResult> future = CompletableFuture.supplyAsync(
                        () -> callMeta(recipient, credentials, campaignId),
                        whatsappExecutor
                );
                futures.add(future);
            }

            // Wait for all to complete (with timeout)
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(300, TimeUnit.SECONDS);

            // Collect results
            List<RecipientResult> results = new ArrayList<>(futures.size());
            for (CompletableFuture<RecipientResult> f : futures) {
                results.add(f.get());
            }
            return results;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("sendToMeta interrupted: campaignId={}", campaignId);
            return buildErrorResults(recipients, campaignId, "Interrupted");

        } catch (Exception e) {
            log.error("sendToMeta failed: campaignId={} error={}", campaignId, e.getMessage(), e);
            return buildErrorResults(recipients, campaignId, e.getMessage());

        } finally {
            // Always release permits
            semaphore.release(acquiredPermits);
            log.debug("Released {} permits for wabaKey={}", acquiredPermits, wabaKey);
        }
    }

    /**
     * Single Meta API call for one recipient.
     */
    private RecipientResult callMeta(RecipientPayload recipient, WabaCredentials credentials, long campaignId) {
        try {
            MetaApiResponse response = whatsappClient.sendMessage(
                    recipient.getRequestPayload(),
                    credentials.getPhoneNumberId(),
                    credentials.getAccessToken()
            );

            if (response != null && response.isSuccess()) {
                return RecipientResult.success(
                        recipient.getRecipientId(),
                        recipient.getMessageId(),
                        recipient.getContactId(),
                        response.getProviderMessageId()
                );
            } else {
                String errMsg = (response != null && response.getError() != null)
                        ? response.getError().getMessage()
                        : "Empty response from Meta";
                String errCode = (response != null && response.getError() != null)
                        ? String.valueOf(response.getError().getCode())
                        : null;
                return RecipientResult.failure(
                        recipient.getRecipientId(),
                        recipient.getMessageId(),
                        recipient.getContactId(),
                        errCode, errMsg
                );
            }

        } catch (Exception e) {
            log.error("Meta API call failed: campaignId={} recipientId={} error={}",
                    campaignId, recipient.getRecipientId(), e.getMessage());
            return RecipientResult.failure(
                    recipient.getRecipientId(),
                    recipient.getMessageId(),
                    recipient.getContactId(),
                    null, e.getMessage()
            );
        }
    }

    /**
     * Report batch results to messaging service asynchronously (fire-and-forget).
     */
    private void reportResultsAsync(BroadcastMessageEvent event, List<RecipientResult> results) {
        List<MessageResultCallbackRequest.RecipientResult> callbackResults = results.stream()
                .map(r -> MessageResultCallbackRequest.RecipientResult.builder()
                        .recipientId(r.recipientId())
                        .messageId(r.messageId())
                        .contactId(r.contactId())
                        .success(r.success())
                        .providerMessageId(r.providerMessageId())
                        .errorCode(r.errorCode())
                        .errorMessage(r.errorMessage())
                        .build())
                .toList();

        MessageResultCallbackRequest callbackRequest = MessageResultCallbackRequest.builder()
                .campaignId(event.getCampaignId())
                .wabaAccountId(event.getWabaAccountId())
                .results(callbackResults)
                .build();

        // Non-blocking — MessagingCallbackClientImpl uses subscribe() internally
        callbackClient.reportResults(callbackRequest);
    }

    private List<RecipientResult> buildErrorResults(
            List<RecipientPayload> recipients,
            long campaignId,
            String errorMessage) {
        return recipients.stream()
                .map(r -> RecipientResult.failure(
                        r.getRecipientId(),
                        r.getMessageId(),
                        r.getContactId(),
                        null, errorMessage))
                .toList();
    }

    // ─── Periodic Cleanup ────────────────────────────────────────────────────

    @Scheduled(fixedRate = 300_000) // Every 5 minutes
    public void cleanupEmptyQueues() {
        int removed = 0;
        for (var entry : wabaQueues.entrySet()) {
            WabaQueue q = entry.getValue();
            if (q.isEmpty() && !q.isProcessing()) {
                if (wabaQueues.remove(entry.getKey(), q)) {
                    removed++;
                }
            }
        }
        if (removed > 0) {
            log.info("Cleaned up {} empty WABA queues. Remaining: {}", removed, wabaQueues.size());
        }
    }

    // ─── Graceful Shutdown ───────────────────────────────────────────────────

    @PreDestroy
    public void shutdown() {
        log.info("BatchCoordinatorService shutting down...");
        shutdownRequested.set(true);

        log.info("Shutdown complete. totalRecipients={} totalBatches={}",
                totalRecipientsProcessed.get(), totalBatchesProcessed.get());
    }

    // ─── Stats ───────────────────────────────────────────────────────────────

    public BatchStats getStats() {
        int activeQueues = 0, processingQueues = 0, totalPending = 0;
        for (WabaQueue q : wabaQueues.values()) {
            if (!q.isEmpty()) { activeQueues++; totalPending += q.size(); }
            if (q.isProcessing()) processingQueues++;
        }
        return new BatchStats(
                wabaQueues.size(), activeQueues, processingQueues, totalPending,
                totalRecipientsProcessed.get(), totalBatchesProcessed.get()
        );
    }

    // ─── Inner Types ─────────────────────────────────────────────────────────

    private static class WabaQueue {
        private final String wabaKey;
        private final ConcurrentLinkedQueue<QueuedBatch> queue = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean processing = new AtomicBoolean(false);

        WabaQueue(String wabaKey) { this.wabaKey = wabaKey; }

        String getWabaKey() { return wabaKey; }
        void enqueue(QueuedBatch b) { queue.offer(b); }
        QueuedBatch poll() { return queue.poll(); }
        boolean isEmpty() { return queue.isEmpty(); }
        int size() { return queue.size(); }
        boolean isProcessing() { return processing.get(); }
        boolean tryStartProcessing() { return processing.compareAndSet(false, true); }
        boolean tryStopProcessing() { return processing.compareAndSet(true, false); }
        void forceStopProcessing() { processing.set(false); }
    }

    private record QueuedBatch(BroadcastMessageEvent event, Acknowledgment acknowledgment) {}

    private record RecipientResult(
            Long recipientId,
            Long messageId,
            Long contactId,
            boolean success,
            String providerMessageId,
            String errorCode,
            String errorMessage) {

        static RecipientResult success(Long recipientId, Long messageId, Long contactId, String providerMessageId) {
            return new RecipientResult(recipientId, messageId, contactId, true, providerMessageId, null, null);
        }

        static RecipientResult failure(Long recipientId, Long messageId, Long contactId, String errorCode, String errorMessage) {
            return new RecipientResult(recipientId, messageId, contactId, false, null, errorCode, errorMessage);
        }
    }

    public record BatchStats(
            int totalQueues, int activeQueues, int processingQueues,
            int totalPending, long totalRecipientsProcessed, long totalBatchesProcessed) {}
}