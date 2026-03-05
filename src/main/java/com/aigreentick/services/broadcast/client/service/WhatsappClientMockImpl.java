package com.aigreentick.services.broadcast.client.service;

import com.aigreentick.services.broadcast.client.dto.MetaApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mock implementation for stress testing.
 * Simulates Meta WhatsApp Cloud API responses without making actual HTTP calls.
 * Active when profile is 'stress-test' or 'mock'.
 */
@Slf4j
@Service
@Profile("stress-test | mock")
public class WhatsappClientMockImpl implements WhatsappClient {

    private static final Random random = new Random();
    private static final AtomicLong messageCounter = new AtomicLong(0);

    private static final int MIN_DELAY_MS = 50;
    private static final int MAX_DELAY_MS = 200;
    private static final double FAILURE_RATE = 0.05;      // 5% failure rate
    private static final double ACCEPTANCE_RATE = 0.95;   // 95% accepted vs sent

    private final AtomicLong totalCalls = new AtomicLong(0);
    private final AtomicLong successCalls = new AtomicLong(0);
    private final AtomicLong failedCalls = new AtomicLong(0);

    @Override
    public MetaApiResponse sendMessage(String requestPayload, String phoneNumberId, String accessToken) {
        long callNumber = totalCalls.incrementAndGet();
        long startTime = System.currentTimeMillis();

        try {
            simulateNetworkDelay();

            boolean shouldFail = random.nextDouble() < FAILURE_RATE;

            if (shouldFail) {
                return handleMockFailure(callNumber, startTime, phoneNumberId);
            } else {
                return handleMockSuccess(callNumber, startTime, phoneNumberId);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Mock call interrupted. callNumber={}", callNumber, e);
            failedCalls.incrementAndGet();
            return buildErrorResponse(500, "Request interrupted");
        }
    }

    private void simulateNetworkDelay() throws InterruptedException {
        int delay = MIN_DELAY_MS + random.nextInt(MAX_DELAY_MS - MIN_DELAY_MS + 1);
        Thread.sleep(delay);
    }

    private MetaApiResponse handleMockSuccess(long callNumber, long startTime, String phoneNumberId) {
        successCalls.incrementAndGet();

        String wamid = generateMessageId();
        String status = random.nextDouble() < ACCEPTANCE_RATE ? "accepted" : "sent";

        MetaApiResponse.ContactDto contact = new MetaApiResponse.ContactDto();
        contact.setInput(phoneNumberId);
        contact.setWaId(phoneNumberId);

        MetaApiResponse.MessageDto message = new MetaApiResponse.MessageDto();
        message.setId(wamid);
        message.setMessageStatus(status);

        MetaApiResponse response = new MetaApiResponse();
        response.setMessagingProduct("whatsapp");
        response.setContacts(List.of(contact));
        response.setMessages(List.of(message));

        long duration = System.currentTimeMillis() - startTime;
        log.debug("[MOCK] Success: callNumber={} wamid={} status={} duration={}ms",
                callNumber, wamid, status, duration);

        return response;
    }

    private MetaApiResponse handleMockFailure(long callNumber, long startTime, String phoneNumberId) {
        failedCalls.incrementAndGet();

        String[] errorMessages = {
                "Rate limit exceeded",
                "Invalid phone number",
                "Template not found",
                "Network timeout"
        };
        int[] errorCodes = { 429, 400, 404, 503 };

        int idx = random.nextInt(errorMessages.length);
        long duration = System.currentTimeMillis() - startTime;

        log.debug("[MOCK] Failure: callNumber={} phoneNumberId={} error={} duration={}ms",
                callNumber, phoneNumberId, errorMessages[idx], duration);

        return buildErrorResponse(errorCodes[idx], errorMessages[idx]);
    }

    private MetaApiResponse buildErrorResponse(int code, String message) {
        MetaApiResponse.ErrorDto error = new MetaApiResponse.ErrorDto();
        error.setCode(code);
        error.setMessage(message);
        error.setType("mock_error");

        MetaApiResponse response = new MetaApiResponse();
        response.setError(error);
        return response;
    }

    private String generateMessageId() {
        long seq = messageCounter.incrementAndGet();
        String uuid = UUID.randomUUID().toString().replace("-", "");
        return String.format("wamid.%s_%d", uuid, seq);
    }

    public MockStatistics getStatistics() {
        return new MockStatistics(totalCalls.get(), successCalls.get(), failedCalls.get(), calculateSuccessRate());
    }

    private double calculateSuccessRate() {
        long total = totalCalls.get();
        return total == 0 ? 0.0 : (successCalls.get() * 100.0) / total;
    }

    public void resetStatistics() {
        totalCalls.set(0);
        successCalls.set(0);
        failedCalls.set(0);
        log.info("[MOCK] Statistics reset");
    }

    public record MockStatistics(long totalCalls, long successCalls, long failedCalls, double successRate) {
        @Override
        public String toString() {
            return String.format("Total: %d, Success: %d, Failed: %d, Rate: %.2f%%",
                    totalCalls, successCalls, failedCalls, successRate);
        }
    }
}