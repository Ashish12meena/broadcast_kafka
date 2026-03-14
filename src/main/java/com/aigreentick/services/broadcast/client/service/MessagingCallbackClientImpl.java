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
    private final MessagingClientProperties props;

    public MessagingCallbackClientImpl(WebClient.Builder builder, MessagingClientProperties props) {
        this.props = props;
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

        webClient.post()
                .uri(props.getPaths().getMessageResultsCallback())
                .bodyValue(request)
                .retrieve()
                .toBodilessEntity()
                .subscribe(
                        response -> log.debug("Callback accepted: campaignId={} status={}",
                                request.getCampaignId(), response.getStatusCode()),
                        error -> log.error("Callback failed: campaignId={} error={}",
                                request.getCampaignId(), error.getMessage())
                );
    }
}