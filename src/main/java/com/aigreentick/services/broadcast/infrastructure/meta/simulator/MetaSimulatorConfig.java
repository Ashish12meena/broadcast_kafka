package com.aigreentick.services.broadcast.infrastructure.meta.simulator;

import io.netty.channel.ChannelOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

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
     * A separate client from {@code metaWebClient}, with its own explicitly sized pool.
     *
     * <p>The separation was always intended — sharing the Meta pool would let a slow callback
     * endpoint consume the connections the send path is sized around, so the simulated webhook
     * would throttle the sends it is reporting on. But building this with a bare
     * {@code WebClient.builder()} did not achieve it. With no connector configured, Reactor Netty
     * falls through to its global shared pool, which defaults to {@code max(cores, 8) x 2} — around
     * sixteen connections, a quarter of the sixty-four deliberately configured for Meta. Three
     * callbacks per recipient against sixteen connections overflows the pending-acquire queue at
     * one thousand, and every request past that is rejected before it is ever sent.
     *
     * <p>{@code responseTimeout} matters more than the pool size. Without it an in-flight request
     * holds its connection indefinitely, so a receiver that hangs rather than refuses wedges the
     * pool permanently instead of briefly.
     */
    @Bean("simulatorCallbackWebClient")
    public WebClient simulatorCallbackWebClient(MetaSimulatorProperties properties) {
        if (properties.callbacksEnabled()) {
            log.info("Simulated delivery statuses will be posted to {} after a random delay of "
                            + "{}-{}ms per status, max {} in flight over {} connections",
                    properties.callbackUrl(),
                    properties.minDelay().toMillis(),
                    properties.maxDelay().toMillis(),
                    properties.maxInFlight(),
                    properties.maxConnections());
        } else {
            log.warn("broadcast.simulator.callback-url is not set. Sends will be simulated, but no "
                    + "delivery statuses will be posted.");
        }

        ConnectionProvider connectionProvider = ConnectionProvider.builder("simulator-callback")
                .maxConnections(properties.maxConnections())
                // Generous relative to maxInFlight, which is the real limit. This only needs to be
                // large enough that a brief burst queues instead of being rejected.
                .pendingAcquireMaxCount(properties.maxConnections() * 20)
                .pendingAcquireTimeout(Duration.ofSeconds(10))
                .maxIdleTime(Duration.ofSeconds(30))
                .build();

        HttpClient httpClient = HttpClient.create(connectionProvider)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                        (int) properties.connectTimeout().toMillis())
                .responseTimeout(properties.responseTimeout());

        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }
}