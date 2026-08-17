package com.aigreentick.services.broadcast.infrastructure.meta.simulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Wires the simulator. Every bean here is confined to the test profile, so there is one place to
 * look when answering "can this deployment reach Meta?" — the answer is the profile, and nothing
 * else.
 */
@Configuration
@Profile("test")
@EnableConfigurationProperties(MetaSimulatorProperties.class)
public class MetaSimulatorConfig {

    private static final Logger log = LoggerFactory.getLogger(MetaSimulatorConfig.class);

    /**
     * A separate client from {@code metaWebClient}. Sharing the Meta pool would let a slow callback
     * endpoint consume the connections the send path is sized around, so the simulated webhook would
     * throttle the sends it is reporting on.
     */
    @Bean("simulatorCallbackWebClient")
    public WebClient simulatorCallbackWebClient(MetaSimulatorProperties properties) {
        if (properties.callbacksEnabled()) {
            log.info("Simulated delivery statuses will be posted to {} after a random delay of "
                            + "{}-{}ms per status",
                    properties.callbackUrl(),
                    properties.minDelay().toMillis(),
                    properties.maxDelay().toMillis());
        } else {
            log.warn("broadcast.simulator.callback-url is not set. Sends will be simulated, but no "
                    + "delivery statuses will be posted.");
        }

        return WebClient.builder()
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }
}
