package com.aigreentick.services.broadcast.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * One block per BSP a batch can be routed to — {@code broadcast.providers.*} (doc 47 R3). Meta is
 * not here: it is {@code broadcast.meta}.
 *
 * <p>No switch and no allowlist: the Messaging Service decides the route and puts it on the batch.
 * This only says how to reach the provider. A new BSP is a new block plus one {@code SendEndpoint}.
 *
 * <pre>
 * broadcast.providers.pinnacle:
 *   base-url: https://partnersv1.pinbot.ai/v3
 *   connect-timeout: 3s
 *   read-timeout: 15s
 *   max-connections: 32
 *   pending-acquire-timeout: 5s
 *   idle-timeout: 30s
 * </pre>
 */
@ConfigurationProperties(prefix = "broadcast")
public record ProviderSendProperties(Map<String, Provider> providers) {

    public ProviderSendProperties {
        providers = providers == null ? Map.of() : Map.copyOf(providers);
    }

    /** The block for this provider code, matched case-insensitively. */
    public Optional<Provider> find(String providerCode) {
        return providerCode == null
                ? Optional.empty()
                : Optional.ofNullable(providers.get(providerCode.toLowerCase(Locale.ROOT)));
    }

    public record Provider(
            String baseUrl,
            Duration connectTimeout,
            Duration readTimeout,
            Integer maxConnections,
            Duration pendingAcquireTimeout,
            Duration idleTimeout) {

        public Provider {
            connectTimeout = connectTimeout == null ? Duration.ofSeconds(3) : connectTimeout;
            readTimeout = readTimeout == null ? Duration.ofSeconds(15) : readTimeout;
            maxConnections = maxConnections == null || maxConnections < 1 ? 32 : maxConnections;
            pendingAcquireTimeout = pendingAcquireTimeout == null ? Duration.ofSeconds(5) : pendingAcquireTimeout;
            idleTimeout = idleTimeout == null ? Duration.ofSeconds(30) : idleTimeout;
        }
    }
}
