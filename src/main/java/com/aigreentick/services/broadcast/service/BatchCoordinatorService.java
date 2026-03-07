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
 * 3. processBatch() — partition 1000 recipients into windows of WINDOW_SIZE (80).
 * 4. sendWindow() — send 80 to Meta concurrently, wait for all 80 responses.
 * 5. reportWindow() — immediately callback Messaging Service with those 80 results.
 * 6. repeat 4-5 — until all windows done, then acknowledge Kafka offset.
 */
@Slf4j
@Service
public class BatchCoordinatorService {

    private static final int WINDOW_SIZE = ConfigConstants.MAX_CONCURRENT_WHATSAPP_REQUESTS; // 80

    private final WhatsappClient whatsappClient;
    private final MessagingCallbackClient callbackClient;
    private final ExecutorService whatsappExecutor;

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

        if (queue.tryStartProcessing()) {
            whatsappExecutor.submit(() -> drainQueue(queue));
        }
    }

    // ─── Queue Drain Loop ─────────────────────────────────────────────────────

    private void drainQueue(PhoneQueue queue) {
        String phoneNumberId = queue.getPhoneNumberId();
        log.debug("Worker started: phoneNumberId={}", phoneNumberId);

        try {
            while (!shutdownRequested.get()) {
                QueuedBatch batch = queue.poll();

                if (batch == null) {
                    if (queue.tryStopProcessing()) {
                        if (queue.isEmpty()) {
                            log.debug("Worker exiting: phoneNumberId={} queue empty", phoneNumberId);
                            return;
                        }
                        if (queue.tryStartProcessing()) {
                            continue;
                        }
                        return;
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

                List<RecipientResult> windowResults = sendWindow(phoneNumberId, accessToken, window, campaignId);

                reportWindowResults(campaignId, phoneNumberId, windowResults);

                long successCount = windowResults.stream().filter(RecipientResult::success).count();
                totalSuccess += (int) successCount;
                totalFailed += windowResults.size() - (int) successCount;
                totalWindowsProcessed.incrementAndGet();

                log.debug("Window {}/{} done: campaignId={} success={} failed={}",
                        i + 1, totalWindows, campaignId, successCount,
                        windowResults.size() - successCount);
            }

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
     *
     * Now captures messageStatus from Meta response for accurate status tracking.
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
                // Extract messageStatus from Meta response (e.g. "accepted", "sent")
                String messageStatus = null;
                if (response.getMessages() != null && !response.getMessages().isEmpty()) {
                    messageStatus = response.getMessages().get(0).getMessageStatus();
                }

                return RecipientResult.success(
                        recipient.getRecipientId(),
                        recipient.getMessageId(),
                        recipient.getContactId(),
                        response.getProviderMessageId(),
                        messageStatus);                     // ← NEW: pass messageStatus
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
     * Uses camelCase — inter-service convention.
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
                        .messageStatus(r.messageStatus())           // ← NEW
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

    @Scheduled(fixedRate = 300_000)
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

    /**
     * Updated: added messageStatus field to carry Meta's status string.
     */
    private record RecipientResult(
            Long recipientId,
            Long messageId,
            Long contactId,
            boolean success,
            String providerMessageId,
            String messageStatus,           // ← NEW
            String errorCode,
            String errorMessage) {

        static RecipientResult success(Long recipientId, Long messageId, Long contactId,
                                        String wamid, String messageStatus) {
            return new RecipientResult(recipientId, messageId, contactId, true,
                    wamid, messageStatus, null, null);
        }

        static RecipientResult failure(Long recipientId, Long messageId, Long contactId,
                String errorCode, String errorMessage) {
            return new RecipientResult(recipientId, messageId, contactId, false,
                    null, null, errorCode, errorMessage);
        }
    }

    public record BatchStats(
            int totalQueues, int activeQueues, int processingQueues, int totalPending,
            long totalBatchesProcessed, long totalWindowsProcessed,
            long totalRecipientsProcessed, long totalCallbacksSent) {
    }
}