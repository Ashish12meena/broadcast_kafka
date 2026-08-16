package com.aigreentick.services.broadcast;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * Verifies the context wires together.
 *
 * <p>Disabled by default because it needs a live Kafka and Redis. Run it against the local
 * docker-compose stack, or bring in Testcontainers if this should run in CI.
 */
@SpringBootTest
@ActiveProfiles("test")
@org.junit.jupiter.api.Disabled("requires Kafka and Redis; see docker-compose.yml")
class BroadcastApplicationTests {

    @Test
    void contextLoads() {
    }
}
