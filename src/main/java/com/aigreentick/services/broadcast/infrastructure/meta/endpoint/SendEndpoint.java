package com.aigreentick.services.broadcast.infrastructure.meta.endpoint;

import com.aigreentick.services.broadcast.domain.model.SendCredential;
import com.aigreentick.services.broadcast.domain.model.SendResponse;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * What differs between the providers a batch can be sent through — one implementation per provider
 * (strategy), picked by {@link SendEndpointRegistry} from the batch's {@link SendCredential}.
 *
 * <p>The send itself — {@code POST /{phoneNumberId}/messages} with the rendered payload, response
 * parsing, the transport-failure rules — is shared in {@code MetaCloudApiClient}, because the
 * providers we support mirror Meta's Cloud API (doc 47 §1).
 *
 * <h2>Adding a provider</h2>
 * One class implementing this, plus a {@code broadcast.providers.<code>} block (doc 47 §4.7).
 */
public interface SendEndpoint {

    /** The provider code it serves, as the batch names it ({@code META_DIRECT}, {@code PINNACLE}). */
    String provider();

    /** For messages and logs: "Meta", "Pinnacle". */
    String displayName();

    /** The client for this provider, with its own connection pool. */
    WebClient webClient();

    /** Sets this provider's authentication on the request. Never logs the secret. */
    void authenticate(HttpHeaders headers, SendCredential credential);

    /** The request body for a send, from the Meta-format payload. Identity for Meta-compatible providers. */
    default String toRequestBody(String metaPayload) {
        return metaPayload;
    }

    /**
     * An error response that carries no Meta error code — read by HTTP status, as Meta-equivalent
     * codes so {@code MetaErrorCatalog} and the rate degrader treat it like Meta's:
     * <ul>
     *   <li>401 / 403 → 190 (credential; messaging re-dispatches with a fresh credential)</li>
     *   <li>429 → 130429 (throughput limit; the number is slowed down)</li>
     *   <li>5xx → unreachable (transient; counts against the number's circuit breaker)</li>
     *   <li>other → rejected without a code (permanent)</li>
     * </ul>
     */
    default SendResponse uncodedError(int httpStatus) {
        String detail = displayName() + " returned HTTP " + httpStatus;
        if (httpStatus == 401 || httpStatus == 403) {
            return SendResponse.rejected(190, detail + " (credential refused)");
        }
        if (httpStatus == 429) {
            return SendResponse.rejected(130429, detail + " (rate limited)");
        }
        if (httpStatus >= 500) {
            return SendResponse.unreachable(detail);
        }
        return SendResponse.rejected(null, detail);
    }
}
