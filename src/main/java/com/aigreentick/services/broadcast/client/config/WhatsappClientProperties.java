package com.aigreentick.services.broadcast.client.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "meta.api")
public class WhatsappClientProperties {

    /** e.g. https://graph.facebook.com/v23.0 */
    private String baseUrl = "https://graph.facebook.com/v23.0";

    private int connectTimeout = 5000;
    private int readTimeout = 30000;
}