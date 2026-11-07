package com.aigreentick.services.broadcast.infrastructure.meta;

import com.aigreentick.services.broadcast.domain.model.SendCredential;
import com.aigreentick.services.broadcast.domain.model.SendResponse;
import com.aigreentick.services.broadcast.domain.policy.MetaErrorCatalog;
import com.aigreentick.services.broadcast.domain.policy.MetaErrorClass;
import com.aigreentick.services.broadcast.infrastructure.config.ProviderSendProperties;
import com.aigreentick.services.broadcast.infrastructure.config.WebClientConfig;
import com.aigreentick.services.broadcast.infrastructure.meta.endpoint.MetaSendEndpoint;
import com.aigreentick.services.broadcast.infrastructure.meta.endpoint.PinnacleSendEndpoint;
import com.aigreentick.services.broadcast.infrastructure.meta.endpoint.SendEndpointRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Campaign sends routed by the batch's credential, over real HTTP (doc 47 R3): Pinnacle's host and
 * {@code apikey} for a Pinnacle batch, Meta and Bearer otherwise, errors classified like Meta's.
 */
class MetaCloudApiClientRoutingTest {

    private static final String PAYLOAD = """
            {"messaging_product":"whatsapp","to":"919876543210","type":"template",
             "template":{"name":"t","language":{"code":"en"}},"biz_opaque_callback_data":"cmp:1:2"}""";
    private static final String ACCEPTED = """
            {"messaging_product":"whatsapp","messages":[{"id":"wamid.PIN","message_status":"accepted"}]}""";
    private static final SendCredential PINNACLE = new SendCredential("PINNACLE", "meta-token", "pin-key", "p-1");
    private static final SendCredential META = SendCredential.meta("meta-token");

    private final AtomicReference<String> path = new AtomicReference<>();
    private final AtomicReference<Headers> headers = new AtomicReference<>();
    private final AtomicReference<String> body = new AtomicReference<>();
    private final AtomicInteger calls = new AtomicInteger();
    private volatile int status = 200;
    private volatile String contentType = "application/json";
    private volatile String response = ACCEPTED;

    private HttpServer server;
    private MetaCloudApiClient client;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            calls.incrementAndGet();
            path.set(exchange.getRequestURI().getPath());
            headers.set(exchange.getRequestHeaders());
            body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.start();
        String base = "http://127.0.0.1:" + server.getAddress().getPort();

        var meta = WebClientConfig.build("meta-test", base + "/v23.0", Duration.ofSeconds(2), Duration.ofSeconds(5),
                4, Duration.ofSeconds(2), Duration.ofSeconds(30));
        var providers = new ProviderSendProperties(Map.of("pinnacle", new ProviderSendProperties.Provider(
                base + "/v3", Duration.ofSeconds(2), Duration.ofSeconds(5), 4, Duration.ofSeconds(2), Duration.ofSeconds(30))));

        client = new MetaCloudApiClient(new SendEndpointRegistry(List.of(
                new MetaSendEndpoint(meta), new PinnacleSendEndpoint(providers))), new ObjectMapper());
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private void answer(int status, String contentType, String response) {
        this.status = status;
        this.contentType = contentType;
        this.response = response;
    }

    @Test
    void aPinnacleBatchGoesToPinnacleWithTheApiKeyAndTheSamePayload() {
        SendResponse sent = client.send("pn-174", 21L, PINNACLE, PAYLOAD);

        assertThat(sent.success()).isTrue();
        assertThat(sent.providerMessageId()).isEqualTo("wamid.PIN");
        assertThat(path.get()).isEqualTo("/v3/pn-174/messages");
        assertThat(headers.get().getFirst("apikey")).isEqualTo("pin-key");
        assertThat(headers.get().containsKey("Authorization")).isFalse();
        assertThat(body.get()).isEqualTo(PAYLOAD);
    }

    @Test
    void aMetaBatchIsUnchangedBearerOnGraph() {
        SendResponse sent = client.send("pn-17", 5L, META, PAYLOAD);

        assertThat(sent.success()).isTrue();
        assertThat(path.get()).isEqualTo("/v23.0/pn-17/messages");
        assertThat(headers.get().getFirst("Authorization")).isEqualTo("Bearer meta-token");
        assertThat(headers.get().containsKey("apikey")).isFalse();
    }

    @Test
    void aBatchWithoutAProviderIsMeta() {
        client.send("pn-17", 5L, new SendCredential(null, "meta-token", null, null), PAYLOAD);

        assertThat(path.get()).isEqualTo("/v23.0/pn-17/messages");
    }

    @Test
    void aPinnacleBatchWithoutAKeyIsNeverSentAndIsACredentialFailure() {
        SendResponse response = client.send("pn-174", 21L,
                new SendCredential("PINNACLE", "meta-token", null, null), PAYLOAD);

        assertThat(calls.get()).isZero();
        assertThat(MetaErrorCatalog.classify(response.errorCode())).isEqualTo(MetaErrorClass.CREDENTIAL);
    }

    @Test
    void anUnknownProviderIsNeverSentAndIsRetryable() {
        SendResponse response = client.send("pn-1", 1L, new SendCredential("NEWBSP", "t", "k", null), PAYLOAD);

        assertThat(calls.get()).isZero();
        assertThat(response.transportFailure()).isTrue();
        assertThat(response.errorMessage()).contains("No send endpoint for provider NEWBSP");
    }

    @Test
    void aMetaShapedErrorFromPinnacleIsClassifiedByItsCode() {
        answer(400, "application/json", "{\"error\":{\"code\":131026,\"message\":\"Message undeliverable\"}}");

        SendResponse response = client.send("pn-174", 21L, PINNACLE, PAYLOAD);

        assertThat(response.success()).isFalse();
        assertThat(response.errorCode()).isEqualTo(131026);
    }

    @Test
    void uncodedPinnacleErrorsAreReadByStatus() {
        answer(401, "text/plain", "Unauthorized");
        assertThat(MetaErrorCatalog.classify(client.send("pn-174", 21L, PINNACLE, PAYLOAD).errorCode()))
                .isEqualTo(MetaErrorClass.CREDENTIAL);

        answer(429, "text/plain", "slow down");
        assertThat(MetaErrorCatalog.classify(client.send("pn-174", 21L, PINNACLE, PAYLOAD).errorCode()))
                .isEqualTo(MetaErrorClass.RATE_LIMIT);

        answer(502, "text/html", "<html>bad gateway</html>");
        assertThat(client.send("pn-174", 21L, PINNACLE, PAYLOAD).transportFailure()).isTrue();

        answer(400, "application/json", "{\"status\":\"error\",\"message\":\"bad number\"}");
        SendResponse permanent = client.send("pn-174", 21L, PINNACLE, PAYLOAD);
        assertThat(permanent.errorCode()).isNull();
        assertThat(MetaErrorCatalog.classify(permanent.errorCode())).isEqualTo(MetaErrorClass.PERMANENT);
        assertThat(permanent.errorMessage()).isEqualTo("Pinnacle returned HTTP 400");
    }

    @Test
    void anAcceptedSendWithAnUnreadableBodyIsAnUnknownOutcomeNotARetry() {
        answer(200, "text/html", "<html>ok</html>");

        SendResponse response = client.send("pn-174", 21L, PINNACLE, PAYLOAD);

        assertThat(response.outcomeUnknown()).isTrue();
    }

    @Test
    void anAcceptedAnswerInTextJavascriptIsStillRead() {
        answer(200, "text/javascript; charset=UTF-8", ACCEPTED);

        SendResponse response = client.send("pn-17", 5L, META, PAYLOAD);

        assertThat(response.success()).isTrue();
        assertThat(response.providerMessageId()).isEqualTo("wamid.PIN");
    }

    @Test
    void twoEndpointsForOneProviderFailAtStartup() {
        var providers = new ProviderSendProperties(Map.of("pinnacle", new ProviderSendProperties.Provider(
                "http://x/v3", null, null, null, null, null)));

        assertThatThrownBy(() -> new SendEndpointRegistry(List.of(
                new PinnacleSendEndpoint(providers), new PinnacleSendEndpoint(providers))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Two send endpoints for provider PINNACLE");
    }
}
