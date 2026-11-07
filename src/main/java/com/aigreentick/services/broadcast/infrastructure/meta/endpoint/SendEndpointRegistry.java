package com.aigreentick.services.broadcast.infrastructure.meta.endpoint;

import com.aigreentick.services.broadcast.common.constants.InfraConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * The {@link SendEndpoint} strategies, keyed by provider code. Two endpoints for one provider fail
 * the application at startup: which one sent would otherwise depend on bean order.
 */
@Component
@Profile(InfraConstants.Profile.NOT_TEST)
public class SendEndpointRegistry {

    private static final Logger log = LoggerFactory.getLogger(SendEndpointRegistry.class);

    private final Map<String, SendEndpoint> byProvider;

    public SendEndpointRegistry(List<SendEndpoint> endpoints) {
        Map<String, SendEndpoint> map = new LinkedHashMap<>();
        for (SendEndpoint endpoint : endpoints) {
            SendEndpoint previous = map.put(key(endpoint.provider()), endpoint);
            if (previous != null) {
                throw new IllegalStateException("Two send endpoints for provider " + endpoint.provider()
                        + ": " + previous.getClass().getName() + " and " + endpoint.getClass().getName());
            }
        }
        this.byProvider = Collections.unmodifiableMap(map);
        log.info("Send endpoints registered for providers {}", byProvider.keySet());
    }

    public Optional<SendEndpoint> find(String provider) {
        return provider == null ? Optional.empty() : Optional.ofNullable(byProvider.get(key(provider)));
    }

    private static String key(String provider) {
        return provider.toUpperCase(Locale.ROOT);
    }
}
