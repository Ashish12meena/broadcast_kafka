package com.aigreentick.services.broadcast.infrastructure.meta.endpoint;

import com.aigreentick.services.broadcast.common.constants.APIPaths;
import com.aigreentick.services.broadcast.common.constants.InfraConstants;
import com.aigreentick.services.broadcast.domain.model.SendCredential;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

/** Meta Graph ({@code broadcast.meta}), bearer token. Every batch without a BSP route. */
@Component
@Profile(InfraConstants.Profile.NOT_TEST)
public class MetaSendEndpoint implements SendEndpoint {

    private final WebClient metaWebClient;

    public MetaSendEndpoint(@Qualifier("metaWebClient") WebClient metaWebClient) {
        this.metaWebClient = metaWebClient;
    }

    @Override
    public String provider() {
        return SendCredential.META_DIRECT;
    }

    @Override
    public String displayName() {
        return "Meta";
    }

    @Override
    public WebClient webClient() {
        return metaWebClient;
    }

    @Override
    public void authenticate(HttpHeaders headers, SendCredential credential) {
        headers.set(HttpHeaders.AUTHORIZATION, APIPaths.BEARER_PREFIX + credential.accessToken());
    }
}
