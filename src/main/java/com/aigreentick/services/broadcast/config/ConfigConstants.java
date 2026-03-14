package com.aigreentick.services.broadcast.config;

/**
 * Shared constants used across executor and batch coordinator configuration.
 */
public final class ConfigConstants {

    private ConfigConstants() {}

    /**
     * Max concurrent WhatsApp API requests per WABA account.
     * Meta allows up to 80 concurrent requests per WABA account.
     */
    public static final int MAX_CONCURRENT_WHATSAPP_REQUESTS = 50;
}