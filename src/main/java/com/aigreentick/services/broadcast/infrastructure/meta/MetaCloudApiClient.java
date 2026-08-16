package com.aigreentick.services.broadcast.infrastructure.meta;

import com.aigreentick.services.broadcast.application.port.out.MetaSendPort;
import com.aigreentick.services.broadcast.domain.model.SendResponse;
import com.aigreentick.services.broadcast.infrastructure.meta.dto.MetaSendResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

/**
 * Sends one message to Meta's Cloud API.
 *
 * <h2>Never throws</h2>
 * Every failure comes back as a {@link SendResponse}. Sends run alongside each other and an escaping
 * exception on one recipient would take its neighbours with it.
 *
 * <h2>Why a 4xx body is parsed rather than treated as an error</h2>
 * Meta answers almost everything with HTTP 400 and puts the real meaning in a numeric code in the
 * body. "Slow down for a moment" and "this number will never receive WhatsApp messages" are the same
 * status and differ only in that code, so a client that stops at the status cannot tell a retryable
 * condition from a permanent one.
 *
 * <h2>Blocking on purpose</h2>
 * {@code block()} here runs on a virtual thread, where parking is cheap. The alternative — threading
 * reactive types through the dispatch loop — would make the pacing logic considerably harder to read
 * for no gain, since the loop's shape is inherently sequential per phone number.
 */
@Component
public class MetaCloudApiClient implements MetaSendPort {

    private static final Logger log = LoggerFactory.getLogger(MetaCloudApiClient.class);

    /** Stands in for a response with no body, so the caller sees a value rather than a null. */
    private static final MetaSendResponse EMPTY_RESPONSE =
            new MetaSendResponse(null, null, null);

    private final WebClient metaWebClient;

    public MetaCloudApiClient(WebClient metaWebClient) {
        this.metaWebClient = metaWebClient;
    }

    @Override
    public SendResponse send(String phoneNumberId, String accessToken, String requestPayload) {
        try {
            MetaSendResponse response = metaWebClient.post()
                    .uri("/{phoneNumberId}/messages", phoneNumberId)
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                    .bodyValue(requestPayload)
                    // exchangeToMono rather than retrieve(): retrieve() raises on any non-2xx, which
                    // would discard the body. Meta puts the error code that decides retryability
                    // inside that body, so it has to be deserialized rather than thrown away.
                    .exchangeToMono(clientResponse ->
                            clientResponse.bodyToMono(MetaSendResponse.class)
                                    .defaultIfEmpty(EMPTY_RESPONSE))
                    .block();

            if (response == null) {
                return SendResponse.rejected(null, "Empty response from Meta");
            }
            if (response.accepted()) {
                return SendResponse.accepted(response.providerMessageId(), response.messageStatus());
            }

            MetaSendResponse.MetaError error = response.error();
            if (error == null) {
                return SendResponse.rejected(null, "Meta returned neither a message nor an error");
            }
            return SendResponse.rejected(error.code(), error.message());

        } catch (WebClientResponseException e) {
            // Meta answered but the body could not be read as the expected shape.
            log.warn("Unreadable Meta response phoneNumberId={} status={} body={}",
                    phoneNumberId, e.getStatusCode(), truncate(e.getResponseBodyAsString()));
            return SendResponse.rejected(null, "Meta returned " + e.getStatusCode().value());

        } catch (WebClientRequestException e) {
            // Meta could not be reached: connection refused, DNS, pool exhaustion, timeout. This is
            // the only class of failure that should influence the circuit breaker.
            log.warn("Meta unreachable phoneNumberId={} reason={}", phoneNumberId, e.getMessage());
            return SendResponse.unreachable(e.getMessage());

        } catch (RuntimeException e) {
            log.error("Unexpected failure calling Meta phoneNumberId={}", phoneNumberId, e);
            return SendResponse.unreachable(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private static String truncate(String body) {
        if (body == null) {
            return "";
        }
        return body.length() <= 512 ? body : body.substring(0, 512) + "...";
    }
}
