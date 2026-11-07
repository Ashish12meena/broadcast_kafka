package com.aigreentick.services.broadcast.infrastructure.meta;

import com.aigreentick.services.broadcast.common.constants.DomainConstants;
import com.aigreentick.services.broadcast.common.constants.InfraConstants;
import com.aigreentick.services.broadcast.application.port.out.MetaSendPort;
import com.aigreentick.services.broadcast.common.constants.APIPaths;
import com.aigreentick.services.broadcast.domain.model.SendCredential;
import com.aigreentick.services.broadcast.domain.model.SendResponse;
import com.aigreentick.services.broadcast.infrastructure.meta.endpoint.SendEndpoint;
import com.aigreentick.services.broadcast.infrastructure.meta.endpoint.SendEndpointRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.aigreentick.services.broadcast.infrastructure.meta.dto.MetaSendResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClientRequestException;

import javax.net.ssl.SSLHandshakeException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;

/**
 * Sends one message to Meta's Cloud API — directly, or through the BSP the batch is routed to.
 *
 * <h2>Providers (doc 47 R3)</h2>
 * The batch's {@link SendCredential} names the provider; {@link SendEndpointRegistry} supplies its
 * {@link SendEndpoint} (host, connection pool, authentication). The path, payload, parsing and the
 * transport rules below are shared: the BSPs we support mirror Meta's API. An unknown provider or a
 * missing key never falls back to Meta.
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
 * {@code @Profile(InfraConstants.Profile.NOT_TEST)} is what guarantees that a test-profile deployment
 * cannot reach Meta: under that profile this bean does not exist, so there is no path to the
 * network. That is a stronger guarantee than an {@code if} inside the send method, which would leave
 * a live client wired up and one mistaken flag away from sending real messages.
 */
@Component
@Profile(InfraConstants.Profile.NOT_TEST)
public class MetaCloudApiClient implements MetaSendPort {

    private static final Logger log = LoggerFactory.getLogger(MetaCloudApiClient.class);

    /** Meta's credential-refused code, used when a batch arrives without the secret its route needs. */
    private static final int CREDENTIAL_REFUSED_CODE = 190;

    private final SendEndpointRegistry endpoints;
    private final ObjectMapper objectMapper;

    public MetaCloudApiClient(SendEndpointRegistry endpoints, ObjectMapper objectMapper) {
        this.endpoints = endpoints;
        this.objectMapper = objectMapper;
    }

    @Override
    public SendResponse send(
            String phoneNumberId, Long wabaAccountId, SendCredential credential, String requestPayload) {
        // wabaAccountId is deliberately unused: the send is addressed by phone number id.
        SendEndpoint endpoint = endpoints.find(credential.provider()).orElse(null);
        if (endpoint == null) {
            // A batch routed to a provider this instance does not know: an older build during a
            // deploy, most likely. Transient, so the batch is retried rather than failed for good.
            log.warn("No send endpoint for provider={} phoneNumberId={}", credential.provider(), phoneNumberId);
            return SendResponse.unreachable("No send endpoint for provider " + credential.provider());
        }
        if (!credential.usable()) {
            // Never sent without its credential and never sent through Meta instead (doc 47 section 2).
            // Classified as a credential failure, so the Messaging Service re-dispatches with a
            // freshly fetched one.
            log.error("Batch carries no usable {} credential phoneNumberId={}", endpoint.displayName(), phoneNumberId);
            return SendResponse.rejected(CREDENTIAL_REFUSED_CODE,
                    "No usable " + endpoint.displayName() + " credential on the batch");
        }

        try {
            Answer answer = endpoint.webClient().post()
                    .uri(APIPaths.META_SEND_MESSAGE, phoneNumberId)
                    .headers(headers -> endpoint.authenticate(headers, credential))
                    .bodyValue(endpoint.toRequestBody(requestPayload))
                    // exchangeToMono rather than retrieve(): retrieve() raises on any non-2xx, which
                    // would discard the body. Meta puts the error code that decides retryability
                    // inside that body. Read as text and parsed here, so an answer that is not JSON
                    // (a BSP's plain-text error, a proxy's HTML page, text/javascript from Graph) is
                    // classified instead of escaping as a decoding failure.
                    .exchangeToMono(clientResponse -> clientResponse.bodyToMono(String.class)
                            .defaultIfEmpty("")
                            .map(body -> new Answer(clientResponse.statusCode().value(), body)))
                    .block();

            return toSendResponse(answer, endpoint, phoneNumberId);

        } catch (WebClientRequestException e) {
            // A transport failure, the only class that should influence the circuit breaker. Which
            // side of the request it happened on decides whether a retry is safe.
            if (failedBeforeRequestWasSent(e)) {
                // Connection refused, DNS, TLS handshake, pool exhaustion: nothing reached the provider.
                log.warn("{} unreachable provider={} phoneNumberId={} reason={}",
                        endpoint.displayName(), endpoint.provider(), phoneNumberId, e.getMessage());
                return SendResponse.unreachable(e.getMessage());
            }
            // Connected, then no answer: read timeout, connection closed mid-response. The provider
            // may have accepted the message, and there is no idempotency key, so this must not be retried.
            log.warn("{} send outcome unknown provider={} phoneNumberId={} reason={}",
                    endpoint.displayName(), endpoint.provider(), phoneNumberId, e.getMessage());
            return SendResponse.outcomeUnknown(e.getMessage());

        } catch (RuntimeException e) {
            log.error("Unexpected failure calling {} provider={} phoneNumberId={}",
                    endpoint.displayName(), endpoint.provider(), phoneNumberId, e);
            return SendResponse.unreachable(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private SendResponse toSendResponse(Answer answer, SendEndpoint endpoint, String phoneNumberId) {
        if (answer == null) {
            return SendResponse.rejected(null, DomainConstants.Messages.EMPTY_META_RESPONSE);
        }
        MetaSendResponse envelope = parse(answer.body());
        boolean ok = answer.status() >= 200 && answer.status() < 300;

        if (ok) {
            if (envelope == null) {
                // Accepted, but the body says nothing readable: the message may well be on its way,
                // so this is an unknown outcome, not a failure to retry.
                log.warn("Unreadable {} response to an accepted send phoneNumberId={} body={}",
                        endpoint.displayName(), phoneNumberId, truncate(answer.body()));
                return SendResponse.outcomeUnknown(
                        endpoint.displayName() + " accepted the message but the response could not be read");
            }
            return MetaResponseMapper.toSendResponse(envelope);
        }

        if (envelope != null && envelope.error() != null && envelope.error().code() != null) {
            // Meta's error envelope, from Meta or passed through by a BSP.
            return MetaResponseMapper.toSendResponse(envelope);
        }

        log.warn("{} error without a Meta code provider={} phoneNumberId={} status={} body={}",
                endpoint.displayName(), endpoint.provider(), phoneNumberId, answer.status(), truncate(answer.body()));
        return endpoint.uncodedError(answer.status());
    }

    private MetaSendResponse parse(String body) {
        if (body == null || body.isBlank()) {
            return MetaResponseMapper.EMPTY_RESPONSE;
        }
        try {
            return objectMapper.readValue(body, MetaSendResponse.class);
        } catch (Exception e) {
            return null;
        }
    }

    /** The status and the raw body of one answer. */
    private record Answer(int status, String body) {
    }

    /**
     * Whether the failure happened before any byte of the request left: connecting, resolving,
     * the TLS handshake, or waiting for a pooled connection. Anything else is treated as sent.
     */
    static boolean failedBeforeRequestWasSent(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            // ConnectException covers Netty's ConnectTimeoutException, which extends it.
            if (t instanceof ConnectException || t instanceof UnknownHostException
                    || t instanceof NoRouteToHostException || t instanceof SSLHandshakeException) {
                return true;
            }
            // Reactor Netty shades reactor-pool, so its acquire failures are matched by name.
            String name = t.getClass().getSimpleName();
            if (name.equals("PoolAcquireTimeoutException") || name.equals("PoolAcquirePendingLimitException")) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }

    private static String truncate(String body) {
        if (body == null) {
            return "";
        }
        return body.length() <= DomainConstants.Meta.RESPONSE_BODY_LOG_LIMIT
                ? body
                : body.substring(0, DomainConstants.Meta.RESPONSE_BODY_LOG_LIMIT)
                        + DomainConstants.Meta.TRUNCATION_SUFFIX;
    }
}
