package com.aigreentick.services.broadcast.infrastructure.config;

import com.aigreentick.services.broadcast.common.constants.DomainConstants;
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

    /**
     * Headroom for a brief burst to queue rather than be rejected. The real ceiling is
     * {@code max-connections}; this only governs how many requests may wait for one.
     */
    private static final int PENDING_ACQUIRE_MULTIPLIER = 4;

    @Bean
    public WebClient metaWebClient(BroadcastProperties properties) {
        BroadcastProperties.Meta meta = properties.meta();
        return build(DomainConstants.Meta.CONNECTION_POOL_NAME, meta.baseUrl(), meta.connectTimeout(),
                meta.readTimeout(), meta.maxConnections(), meta.pendingAcquireTimeout(), meta.idleTimeout());
    }

    /**
     * A client for one provider, with its own explicitly sized connection pool — Meta's above, and
     * each BSP's ({@code broadcast.providers.<code>}, built by its {@code SendEndpoint}). Separate
     * pools so a slow BSP cannot take connections from Meta sends, or the other way round.
     *
     * @param poolName what the pool's metrics are tagged with
     */
    public static WebClient build(String poolName, String baseUrl, Duration connectTimeout, Duration readTimeout,
                                  int maxConnections, Duration pendingAcquireTimeout, Duration idleTimeout) {
        ConnectionProvider connectionProvider =
                ConnectionProvider.builder(poolName)
                .maxConnections(maxConnections)
                .pendingAcquireMaxCount(maxConnections * PENDING_ACQUIRE_MULTIPLIER)
                .pendingAcquireTimeout(pendingAcquireTimeout)
                .maxIdleTime(idleTimeout)
                .metrics(true)
                .build();

        HttpClient httpClient = HttpClient.create(connectionProvider)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout.toMillis())
                .responseTimeout(readTimeout)
                .compress(true);

        return WebClient.builder()
                .baseUrl(baseUrl)
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .codecs(codecs -> codecs.defaultCodecs()
                        .maxInMemorySize(DomainConstants.Meta.MAX_IN_MEMORY_RESPONSE_BYTES))
                .build();
    }
}
