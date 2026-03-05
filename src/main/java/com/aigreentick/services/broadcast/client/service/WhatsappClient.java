package com.aigreentick.services.broadcast.client.service;

import com.aigreentick.services.broadcast.client.dto.MetaApiResponse;

/**
 * Port for sending messages to Meta's WhatsApp Cloud API.
 */
public interface WhatsappClient {

    /**
     * POST /{phoneNumberId}/messages
     *
     * @param requestPayload complete Meta API request body (JSON string, snake_case)
     * @param phoneNumberId  the sending phone number ID
     * @param accessToken    Bearer token for this WABA account
     * @return Meta's response
     */
    MetaApiResponse sendMessage(String requestPayload, String phoneNumberId, String accessToken);
}