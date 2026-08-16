package com.aigreentick.services.broadcast.infrastructure.config;

import io.netty.channel.ChannelOption;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

/**
 * The HTTP client for Meta.
 *
 * <p>The connection pool is configured explicitly and that is the important part of this class.
 * Reactor Netty's default pool is {@code max(cores, 8) x 2} connections — sixteen on a four-core
 * pod. Left at the default it silently caps throughput below every other limit in the system, and
 * because the excess requests queue inside the pool rather than failing, the symptom is rising
 * latency and timeouts rather than anything that names the real cause.
 *
 * <p>Size it from {@code mps x p99 latency} for one instance's share of the traffic, and keep it at
 * or above {@code broadcast.dispatch.max-concurrent-sends}.
 */
@Configuration
public class WebClientConfig {

    @Bean
    public WebClient metaWebClient(BroadcastProperties properties) {
        BroadcastProperties.Meta meta = properties.meta();

        ConnectionProvider connectionProvider = ConnectionProvider.builder("meta")
                .maxConnections(meta.maxConnections())
                .pendingAcquireMaxCount(meta.maxConnections() * 4)
                .pendingAcquireTimeout(meta.pendingAcquireTimeout())
                .maxIdleTime(meta.idleTimeout())
                .metrics(true)
                .build();

        HttpClient httpClient = HttpClient.create(connectionProvider)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) meta.connectTimeout().toMillis())
                .responseTimeout(meta.readTimeout())
                .compress(true);

        return WebClient.builder()
                .baseUrl(meta.baseUrl())
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(256 * 1024))
                .build();
    }

    /** Duration constant kept here so tests can reference the same value. */
    public static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(15);
}
