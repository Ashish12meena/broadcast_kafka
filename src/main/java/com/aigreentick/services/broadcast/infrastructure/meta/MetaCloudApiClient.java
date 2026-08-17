package com.aigreentick.services.broadcast.infrastructure.meta;

import com.aigreentick.services.broadcast.application.port.out.MetaSendPort;
import com.aigreentick.services.broadcast.domain.model.SendResponse;
import com.aigreentick.services.broadcast.infrastructure.meta.dto.MetaSendResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
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
 *
 * <h2>Not registered under the test profile</h2>
 * {@code @Profile("!test")} is what guarantees that a test-profile deployment cannot reach Meta:
 * under that profile this bean does not exist, so there is no path from the dispatch loop to the
 * network. That is a stronger guarantee than an {@code if} inside the send method, which would leave
 * a live client wired up and one mistaken flag away from sending real messages.
 */
@Component
@Profile("!test")
public class MetaCloudApiClient implements MetaSendPort {

    private static final Logger log = LoggerFactory.getLogger(MetaCloudApiClient.class);

    private final WebClient metaWebClient;

    public MetaCloudApiClient(WebClient metaWebClient) {
        this.metaWebClient = metaWebClient;
    }

    @Override
    public SendResponse send(
            String phoneNumberId, Long wabaAccountId, String accessToken, String requestPayload) {
        // wabaAccountId is deliberately unused: Meta addresses the send by phone number id.
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
                                    .defaultIfEmpty(MetaResponseMapper.EMPTY_RESPONSE))
                    .block();

            return MetaResponseMapper.toSendResponse(response);

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
