package com.aigreentick.services.broadcast.client.dto;

import lombok.Builder;
import lombok.Data;

/**
 * Credentials resolved for a WABA account.
 * Used to authenticate calls to Meta's WhatsApp Cloud API.
 */
@Data
@Builder
public class WabaCredentials {
    private Long wabaAccountId;
    private String phoneNumberId;
    private String accessToken;
}