package com.aigreentick.services.broadcast.client.service;

import com.aigreentick.services.broadcast.client.config.WhatsappClientProperties;
import com.aigreentick.services.broadcast.client.dto.MetaApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

@Slf4j
@Component
@Profile("!stress-test & !mock")
public class WhatsappClientRealImpl implements WhatsappClient {

    private final WebClient webClient;

    public WhatsappClientRealImpl(WebClient.Builder builder, WhatsappClientProperties props) {
        this.webClient = builder
                .baseUrl(props.getBaseUrl())
                .defaultHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .build();
    }

    @Override
    public MetaApiResponse sendMessage(String requestPayload, String phoneNumberId, String accessToken) {
        try {
            return webClient.post()
                    .uri("/{phoneNumberId}/messages", phoneNumberId)
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                    .bodyValue(requestPayload)
                    .retrieve()
                    .onStatus(HttpStatusCode::is4xxClientError, clientResponse ->
                            clientResponse.bodyToMono(MetaApiResponse.class)
                                    .map(body -> {
                                        MetaApiResponse.ErrorDto err = body.getError();
                                        String msg = err != null ? err.getMessage() : "4xx from Meta";
                                        return new RuntimeException("Meta API 4xx: " + msg);
                                    }))
                    .onStatus(HttpStatusCode::is5xxServerError, clientResponse ->
                            clientResponse.bodyToMono(String.class)
                                    .map(body -> new RuntimeException("Meta API 5xx: " + body)))
                    .bodyToMono(MetaApiResponse.class)
                    .block();

        } catch (WebClientResponseException e) {
            log.error("Meta API error: status={} body={}", e.getStatusCode(), e.getResponseBodyAsString());
            throw new RuntimeException("Meta API call failed: " + e.getMessage(), e);
        }
    }
}