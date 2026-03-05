package com.aigreentick.services.broadcast.client.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "messaging.service")
public class MessagingClientProperties {

    /** Base URL of the messaging service, e.g. http://messaging-service:8080 */
    private String baseUrl = "http://localhost:8080";
}