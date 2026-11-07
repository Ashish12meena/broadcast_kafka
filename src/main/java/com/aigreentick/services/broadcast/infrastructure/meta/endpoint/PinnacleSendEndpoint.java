package com.aigreentick.services.broadcast.infrastructure.meta.endpoint;

import com.aigreentick.services.broadcast.common.constants.InfraConstants;
import com.aigreentick.services.broadcast.domain.model.SendCredential;
import com.aigreentick.services.broadcast.infrastructure.config.ProviderSendProperties;
import com.aigreentick.services.broadcast.infrastructure.config.WebClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Pinnacle's partner API ({@code https://partnersv1.pinbot.ai/v3}), which mirrors Meta's Cloud API:
 * the same {@code /{phoneNumberId}/messages} path and body, authenticated with the customer's key in
 * an {@code apikey} header (doc 47 §1).
 */
@Component
@Profile(InfraConstants.Profile.NOT_TEST)
public class PinnacleSendEndpoint implements SendEndpoint {

    static final String PROVIDER = "PINNACLE";
    static final String API_KEY_HEADER = "apikey";

    private static final Logger log = LoggerFactory.getLogger(PinnacleSendEndpoint.class);

    private final WebClient webClient;

    public PinnacleSendEndpoint(ProviderSendProperties providers) {
        ProviderSendProperties.Provider config = providers.find(PROVIDER)
                .orElseThrow(() -> new IllegalStateException(
                        "broadcast.providers.pinnacle is missing: the Pinnacle endpoint needs its base-url"));
        this.webClient = WebClientConfig.build("pinnacle", config.baseUrl(), config.connectTimeout(),
                config.readTimeout(), config.maxConnections(), config.pendingAcquireTimeout(), config.idleTimeout());
        log.info("Pinnacle send endpoint baseUrl={} maxConnections={}", config.baseUrl(), config.maxConnections());
    }

    @Override
    public String provider() {
        return PROVIDER;
    }

    @Override
    public String displayName() {
        return "Pinnacle";
    }

    @Override
    public WebClient webClient() {
        return webClient;
    }

    @Override
    public void authenticate(HttpHeaders headers, SendCredential credential) {
        headers.set(API_KEY_HEADER, credential.bspApiKey());
    }
}
