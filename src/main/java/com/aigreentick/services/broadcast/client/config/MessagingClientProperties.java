package com.aigreentick.services.broadcast.client.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;


@Data
@Component
@ConfigurationProperties(prefix = "messaging.service")
public class MessagingClientProperties {

    private String baseUrl = "http://localhost:8050";
    private int connectTimeout = 5000;
    private int readTimeout = 30000;
    private Paths paths = new Paths();

    @Data
    public static class Paths {
        private String messageResultsCallback = "/internal/broadcast/callbacks/message-results";
    }
}