package com.aigreentick.services.broadcast.service;

import com.aigreentick.services.broadcast.client.dto.WabaCredentials;

/**
 * Resolves phone_number_id and access_token for a given WABA account ID.
 *
 * In the messaging service this is done via WabaCredentialPort (HTTP call to account service).
 * Here we need the same — implement via HTTP call to the account/organization service,
 * or via a lightweight cache populated from a config endpoint.
 *
 * Implementations should cache credentials to avoid per-message HTTP calls.
 */
public interface WabaCredentialResolver {

    /**
     * @param wabaAccountId the WABA account ID from the Kafka event
     * @return phone_number_id + access_token to use when calling Meta API
     * @throws RuntimeException if account not found or credentials unavailable
     */
    WabaCredentials resolve(Long wabaAccountId);
}