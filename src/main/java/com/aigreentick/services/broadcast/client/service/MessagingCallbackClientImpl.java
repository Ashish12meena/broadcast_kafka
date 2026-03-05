package com.aigreentick.services.broadcast.client.service;

import com.aigreentick.services.broadcast.client.config.MessagingClientProperties;
import com.aigreentick.services.broadcast.client.dto.MessageResultCallbackRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@Slf4j
@Component
public class MessagingCallbackClientImpl implements MessagingCallbackClient {

    private final WebClient webClient;

    public MessagingCallbackClientImpl(WebClient.Builder builder, MessagingClientProperties props) {
        this.webClient = builder
                .baseUrl(props.getBaseUrl())
                .defaultHeader("Content-Type", "application/json")
                .build();
    }

    @Override
    public void reportResults(MessageResultCallbackRequest request) {
        log.info("Reporting results to messaging service: campaignId={}, results={}",
                request.getCampaignId(),
                request.getResults() != null ? request.getResults().size() : 0);

        // Fire-and-forget: subscribe() without blocking
        webClient.post()
                .uri("/internal/broadcast/callbacks/message-results")
                .bodyValue(request)
                .retrieve()
                .toBodilessEntity()
                .subscribe(
                        response -> log.debug("Callback accepted by messaging service: campaignId={} status={}",
                                request.getCampaignId(), response.getStatusCode()),
                        error -> log.error("Callback to messaging service failed: campaignId={} error={}",
                                request.getCampaignId(), error.getMessage())
                );
    }
}