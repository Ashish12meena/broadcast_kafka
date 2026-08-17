package com.aigreentick.services.broadcast;

import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Centralised WhatsApp send executor.
 *
 * <p>Consumes pre-rendered dispatch batches from the Messaging Service, paces them against Meta's
 * per-phone-number rate limit using a Redis token bucket shared by every instance, and publishes
 * per-recipient outcomes back to Kafka .
 *
 * <p>The service owns no campaign state. All shared state lives in Redis or Kafka, so instances are
 * interchangeable and scaling is a replica-count change.
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(BroadcastProperties.class)
public class BroadcastApplication {

    public static void main(String[] args) {
        SpringApplication.run(BroadcastApplication.class, args);
    }
}
