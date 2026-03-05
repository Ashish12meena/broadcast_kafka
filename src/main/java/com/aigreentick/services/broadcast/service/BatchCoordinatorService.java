package com.aigreentick.services.broadcast.service;

import com.aigreentick.services.broadcast.client.dto.MessageResultCallbackRequest;
import com.aigreentick.services.broadcast.client.dto.MetaApiResponse;
import com.aigreentick.services.broadcast.client.service.MessagingCallbackClient;
import com.aigreentick.services.broadcast.client.service.WhatsappClient;
import com.aigreentick.services.broadcast.config.ConfigConstants;
import com.aigreentick.services.broadcast.kafka.event.BroadcastMessageEvent;
import com.aigreentick.services.broadcast.kafka.event.BroadcastMessageEvent.RecipientPayload;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
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
 * phoneNumberId is the single identifier driving everything:
 * - PhoneQueue key — one queue per phone number, one worker per queue
 * - Semaphore key — Meta enforces 80 concurrent requests per phone number
 * - Meta API param — POST /{phoneNumberId}/messages
 *
 * Processing model per Kafka message (up to 1000 recipients):
 * 1. addBatch() — enqueue, submit worker if none running. Returns immediately.
 * 2. drainQueue() — single worker per phone number drains batches sequentially.
 * 3. processBatch() — partition 1000 recipients into windows of WINDOW_SIZE
 * (80).
 * 4. sendWindow() — send 80 to Meta concurrently, wait for all 80 responses.
 * 5. reportWindow() — immediately callback Messaging Service with those 80
 * results.
 * 6. repeat 4-5 — until all windows done, then acknowledge Kafka offset.
 */
@Slf4j
@Service
public class BatchCoordinatorService {

    /**
     * Recipients sent concurrently per window.
     * Equals Meta's per-phone-number concurrent request limit.
     */
    private static final int WINDOW_SIZE = ConfigConstants.MAX_CONCURRENT_WHATSAPP_REQUESTS; // 80

    private final WhatsappClient whatsappClient;
    private final MessagingCallbackClient callbackClient;
    private final ExecutorService whatsappExecutor;

    // One queue per phoneNumberId — one worker drains each queue sequentially
    private final ConcurrentHashMap<String, PhoneQueue> phoneQueues = new ConcurrentHashMap<>();

    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    // Metrics
    private final AtomicLong totalBatchesProcessed = new AtomicLong(0);
    private final AtomicLong totalWindowsProcessed = new AtomicLong(0);
    private final AtomicLong totalRecipientsProcessed = new AtomicLong(0);
    private final AtomicLong totalCallbacksSent = new AtomicLong(0);

    public BatchCoordinatorService(
            WhatsappClient whatsappClient,
            MessagingCallbackClient callbackClient,
            @Qualifier("whatsappExecutor") ExecutorService whatsappExecutor) {
        this.whatsappClient = whatsappClient;
        this.callbackClient = callbackClient;
        this.whatsappExecutor = whatsappExecutor;
    }

    // ─── Entry Point ─────────────────────────────────────────────────────────

    /**
     * Called by Kafka consumer. Returns immediately — never blocks the consumer
     * thread.
     * Kafka offset is NOT acknowledged here — deferred until all windows complete.
     */
    public void addBatch(BroadcastMessageEvent event, Acknowledgment acknowledgment) {
        if (shutdownRequested.get()) {
            log.warn("Shutdown in progress — acknowledging without processing: campaignId={}",
                    event.getCampaignId());
            acknowledgment.acknowledge();
            return;
        }

        String phoneNumberId = event.getPhoneNumberId();
        PhoneQueue queue = phoneQueues.computeIfAbsent(phoneNumberId, PhoneQueue::new);
        queue.enqueue(new QueuedBatch(event, acknowledgment));

        // Submit a worker only if none is running for this phone number
        if (queue.tryStartProcessing()) {
            whatsappExecutor.submit(() -> drainQueue(queue));
        }
    }

    // ─── Queue Drain Loop ─────────────────────────────────────────────────────

    /**
     * Worker loop: processes all pending batches for one phone number sequentially.
     * Exactly one worker runs per phone number at any time — enforced by
     * PhoneQueue.processing.
     */
    private void drainQueue(PhoneQueue queue) {
        String phoneNumberId = queue.getPhoneNumberId();
        log.debug("Worker started: phoneNumberId={}", phoneNumberId);

        try {
            while (!shutdownRequested.get()) {
                QueuedBatch batch = queue.poll();

                if (batch == null) {
                    // Try to exit — guard against race where new batch arrives just after poll()
                    if (queue.tryStopProcessing()) {
                        if (queue.isEmpty()) {
                            log.debug("Worker exiting: phoneNumberId={} queue empty", phoneNumberId);
                            return;
                        }
                        // New batch arrived between poll() and tryStop() — re-acquire and continue
                        if (queue.tryStartProcessing()) {
                            continue;
                        }
                        return; // Another worker took over
                    }
                    return;
                }

                processBatch(phoneNumberId, batch);
            }
        } catch (Exception e) {
            log.error("Worker failed unexpectedly: phoneNumberId={}", phoneNumberId, e);
        } finally {
            queue.forceStopProcessing();
        }
    }

    // ─── Batch Processing ─────────────────────────────────────────────────────

    /**
     * Processes one Kafka message (up to 1000 recipients) as sequential windows of
     * WINDOW_SIZE.
     *
     * Flow:
     * partition recipients into windows of 80
     * for each window:
     * → sendWindow() — 80 concurrent Meta API calls, wait for all
     * → reportWindow() — callback Messaging Service with 80 results immediately
     * → acknowledge Kafka offset once ALL windows done
     */
    private void processBatch(String phoneNumberId, QueuedBatch queued) {
        BroadcastMessageEvent event = queued.event();
        long campaignId = event.getCampaignId();
        String accessToken = event.getAccessToken();
        List<RecipientPayload> recipients = event.getPayloads();
        int totalWindows = (int) Math.ceil((double) recipients.size() / WINDOW_SIZE);
        long batchStart = System.currentTimeMillis();

        log.info("Processing batch: campaignId={} phoneNumberId={} recipients={} windows={}",
                campaignId, phoneNumberId, recipients.size(), totalWindows);

        int totalSuccess = 0;
        int totalFailed = 0;

        try {
            List<List<RecipientPayload>> windows = partition(recipients, WINDOW_SIZE);

            for (int i = 0; i < windows.size(); i++) {
                if (shutdownRequested.get()) {
                    log.warn("Shutdown detected mid-batch: campaignId={} stoppingAtWindow={}/{}",
                            campaignId, i + 1, totalWindows);
                    break;
                }

                List<RecipientPayload> window = windows.get(i);
                log.debug("Window {}/{}: campaignId={} phoneNumberId={} size={}",
                        i + 1, totalWindows, campaignId, phoneNumberId, window.size());

                // Send this window to Meta — blocks until all responses received
                List<RecipientResult> windowResults = sendWindow(phoneNumberId, accessToken, window, campaignId);

                // Immediately callback Messaging Service with this window's results
                reportWindowResults(campaignId, phoneNumberId, windowResults);

                long successCount = windowResults.stream().filter(RecipientResult::success).count();
                totalSuccess += (int) successCount;
                totalFailed += windowResults.size() - (int) successCount;
                totalWindowsProcessed.incrementAndGet();

                log.debug("Window {}/{} done: campaignId={} success={} failed={}",
                        i + 1, totalWindows, campaignId, successCount,
                        windowResults.size() - successCount);
            }

            // Acknowledge Kafka only after ALL windows complete
            // At-least-once: crash before ack = Kafka redelivers whole batch
            // Messaging Service deduplicates via wamid (provider_message_id)
            queued.acknowledgment().acknowledge();
            log.debug("Kafka acknowledged: campaignId={}", campaignId);

            totalRecipientsProcessed.addAndGet(recipients.size());
            totalBatchesProcessed.incrementAndGet();

            log.info("Batch complete: campaignId={} phoneNumberId={} duration={}ms success={} failed={}",
                    campaignId, phoneNumberId, System.currentTimeMillis() - batchStart,
                    totalSuccess, totalFailed);

        } catch (Exception e) {
            log.error("Batch failed: campaignId={} phoneNumberId={} error={}",
                    campaignId, phoneNumberId, e.getMessage(), e);
            safeAcknowledge(queued, campaignId);
        }
    }

    // ─── Window Sending ───────────────────────────────────────────────────────

    /**
     * Sends one window (≤ WINDOW_SIZE recipients) to Meta fully concurrently.
     *
     * Semaphore:
     * - Keyed by phoneNumberId — Meta's 80 concurrent request limit is per phone
     * number
     * - All permits acquired upfront (window.size() ≤ 80 = total permits)
     * - Safe because exactly one worker runs per phone number — no semaphore
     * contention
     * - Released in finally — no semaphore leaks on timeout or exception
     *
     * @return results for all recipients in this window — never null, never partial
     */
    private List<RecipientResult> sendWindow(
            String phoneNumberId,
            String accessToken,
            List<RecipientPayload> window,
            long campaignId) {

        try {
            List<CompletableFuture<RecipientResult>> futures = new ArrayList<>(window.size());
            for (RecipientPayload recipient : window) {
                futures.add(CompletableFuture.supplyAsync(
                        () -> callMeta(recipient, phoneNumberId, accessToken, campaignId),
                        whatsappExecutor));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(60, TimeUnit.SECONDS);

            List<RecipientResult> results = new ArrayList<>(futures.size());
            for (CompletableFuture<RecipientResult> f : futures) {
                results.add(f.get());
            }
            return results;

        } catch (TimeoutException e) {
            log.error("Window timed out: campaignId={} phoneNumberId={} windowSize={}",
                    campaignId, phoneNumberId, window.size());
            return buildErrorResults(window, "Window timeout after 60s");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Window interrupted: campaignId={} phoneNumberId={}", campaignId, phoneNumberId);
            return buildErrorResults(window, "Interrupted");

        } catch (ExecutionException e) {
            log.error("Window execution failed: campaignId={} phoneNumberId={} error={}",
                    campaignId, phoneNumberId, e.getCause().getMessage());
            return buildErrorResults(window, e.getCause().getMessage());
        }
    }

    // ─── Single Meta API Call ─────────────────────────────────────────────────

    /**
     * One Meta API call for one recipient. Runs on whatsappExecutor.
     * Never throws — always returns success or failure RecipientResult.
     */
    private RecipientResult callMeta(
            RecipientPayload recipient,
            String phoneNumberId,
            String accessToken,
            long campaignId) {
        try {
            MetaApiResponse response = whatsappClient.sendMessage(
                    recipient.getRequestPayload(),
                    phoneNumberId,
                    accessToken);

            if (response != null && response.isSuccess()) {
                return RecipientResult.success(
                        recipient.getRecipientId(),
                        recipient.getMessageId(),
                        recipient.getContactId(),
                        response.getProviderMessageId());
            }

            String errCode = null;
            String errMsg = "Empty response from Meta";
            if (response != null && response.getError() != null) {
                errCode = String.valueOf(response.getError().getCode());
                errMsg = response.getError().getMessage();
            }
            return RecipientResult.failure(
                    recipient.getRecipientId(), recipient.getMessageId(),
                    recipient.getContactId(), errCode, errMsg);

        } catch (Exception e) {
            log.error("Meta API call failed: campaignId={} recipientId={} phoneNumberId={} error={}",
                    campaignId, recipient.getRecipientId(), phoneNumberId, e.getMessage());
            return RecipientResult.failure(
                    recipient.getRecipientId(), recipient.getMessageId(),
                    recipient.getContactId(), null, e.getMessage());
        }
    }

    // ─── Window Callback ──────────────────────────────────────────────────────

    /**
     * Reports results for one window (≤ 80 recipients) to Messaging Service.
     * Fire-and-forget — never blocks the window loop.
     * Called after every window for faster status visibility.
     */
    private void reportWindowResults(
            long campaignId,
            String phoneNumberId,
            List<RecipientResult> results) {

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

        MessageResultCallbackRequest request = MessageResultCallbackRequest.builder()
                .campaignId(campaignId)
                .phoneNumberId(phoneNumberId)
                .results(callbackResults)
                .build();

        callbackClient.reportResults(request);
        totalCallbacksSent.incrementAndGet();
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private <T> List<List<T>> partition(List<T> list, int maxSize) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += maxSize) {
            partitions.add(list.subList(i, Math.min(i + maxSize, list.size())));
        }
        return partitions;
    }

    private List<RecipientResult> buildErrorResults(List<RecipientPayload> recipients, String errorMessage) {
        return recipients.stream()
                .map(r -> RecipientResult.failure(
                        r.getRecipientId(), r.getMessageId(),
                        r.getContactId(), null, errorMessage))
                .toList();
    }

    private void safeAcknowledge(QueuedBatch queued, long campaignId) {
        try {
            queued.acknowledgment().acknowledge();
        } catch (Exception e) {
            log.error("Failed to acknowledge Kafka offset: campaignId={}", campaignId, e);
        }
    }

    // ─── Cleanup ──────────────────────────────────────────────────────────────

    @Scheduled(fixedRate = 300_000) // Every 5 minutes
    public void cleanupEmptyQueues() {
        int removed = 0;
        for (var entry : phoneQueues.entrySet()) {
            PhoneQueue q = entry.getValue();
            if (q.isEmpty() && !q.isProcessing()) {
                if (phoneQueues.remove(entry.getKey(), q)) {
                    removed++;
                }
            }
        }
        if (removed > 0) {
            log.info("Cleaned up {} empty phone queues. Remaining: {}", removed, phoneQueues.size());
        }
    }

    // ─── Graceful Shutdown ────────────────────────────────────────────────────

    @PreDestroy
    public void shutdown() {
        log.info("BatchCoordinatorService shutting down...");
        shutdownRequested.set(true);

        whatsappExecutor.shutdown();
        try {
            if (!whatsappExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                log.warn("whatsappExecutor did not terminate in 60s — forcing shutdown");
                whatsappExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            whatsappExecutor.shutdownNow();
        }

        log.info("Shutdown complete. batches={} windows={} recipients={} callbacks={}",
                totalBatchesProcessed.get(), totalWindowsProcessed.get(),
                totalRecipientsProcessed.get(), totalCallbacksSent.get());
    }

    // ─── Stats ────────────────────────────────────────────────────────────────

    public BatchStats getStats() {
        int activeQueues = 0, processingQueues = 0, totalPending = 0;
        for (PhoneQueue q : phoneQueues.values()) {
            if (!q.isEmpty()) {
                activeQueues++;
                totalPending += q.size();
            }
            if (q.isProcessing())
                processingQueues++;
        }
        return new BatchStats(
                phoneQueues.size(), activeQueues, processingQueues, totalPending,
                totalBatchesProcessed.get(), totalWindowsProcessed.get(),
                totalRecipientsProcessed.get(), totalCallbacksSent.get());
    }

    // ─── Inner Types ──────────────────────────────────────────────────────────

    private static class PhoneQueue {
        private final String phoneNumberId;
        private final ConcurrentLinkedQueue<QueuedBatch> queue = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean processing = new AtomicBoolean(false);

        PhoneQueue(String phoneNumberId) {
            this.phoneNumberId = phoneNumberId;
        }

        String getPhoneNumberId() {
            return phoneNumberId;
        }

        void enqueue(QueuedBatch b) {
            queue.offer(b);
        }

        QueuedBatch poll() {
            return queue.poll();
        }

        boolean isEmpty() {
            return queue.isEmpty();
        }

        int size() {
            return queue.size();
        }

        boolean isProcessing() {
            return processing.get();
        }

        boolean tryStartProcessing() {
            return processing.compareAndSet(false, true);
        }

        boolean tryStopProcessing() {
            return processing.compareAndSet(true, false);
        }

        void forceStopProcessing() {
            processing.set(false);
        }
    }

    private record QueuedBatch(BroadcastMessageEvent event, Acknowledgment acknowledgment) {
    }

    private record RecipientResult(
            Long recipientId,
            Long messageId,
            Long contactId,
            boolean success,
            String providerMessageId,
            String errorCode,
            String errorMessage) {

        static RecipientResult success(Long recipientId, Long messageId, Long contactId, String wamid) {
            return new RecipientResult(recipientId, messageId, contactId, true, wamid, null, null);
        }

        static RecipientResult failure(Long recipientId, Long messageId, Long contactId,
                String errorCode, String errorMessage) {
            return new RecipientResult(recipientId, messageId, contactId, false, null, errorCode, errorMessage);
        }
    }

    public record BatchStats(
            int totalQueues, int activeQueues, int processingQueues, int totalPending,
            long totalBatchesProcessed, long totalWindowsProcessed,
            long totalRecipientsProcessed, long totalCallbacksSent) {
    }
}