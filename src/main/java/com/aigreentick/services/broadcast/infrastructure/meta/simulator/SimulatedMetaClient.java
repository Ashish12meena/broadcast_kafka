package com.aigreentick.services.broadcast.infrastructure.meta.simulator;

import com.aigreentick.services.broadcast.application.port.out.MetaSendPort;
import com.aigreentick.services.broadcast.domain.model.SendResponse;
import com.aigreentick.services.broadcast.infrastructure.meta.MetaResponseMapper;
import com.aigreentick.services.broadcast.infrastructure.meta.dto.MetaSendResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Stands in for Meta's Cloud API under the test profile.
 *
 * <h2>No request leaves the process</h2>
 * This bean replaces {@code MetaCloudApiClient} rather than wrapping it. Under the test profile the
 * real client is not registered at all, so there is no code path from the dispatch loop to
 * graph.facebook.com.
 *
 * <h2>The dummy answer takes the real route home</h2>
 * The response is built as the JSON Meta would have returned, deserialized with the same
 * {@link MetaSendResponse} record, and mapped by the same {@link MetaResponseMapper} the production
 * client uses. The structure handed back is therefore identical by construction rather than by
 * resemblance, which is the point — a simulator that built a {@code SendResponse} directly would
 * skip deserialization and stop exercising the part most likely to break when Meta's envelope moves.
 *
 * <h2>Every send is accepted</h2>
 * There is no failure injection. Meta's error paths are already covered by {@code MetaErrorCatalog}
 * and its tests; what this simulator exists to exercise is the pacing, the bookkeeping and the
 * status round trip.
 */
@Component
@Profile("test")
public class SimulatedMetaClient implements MetaSendPort {

    private static final Logger log = LoggerFactory.getLogger(SimulatedMetaClient.class);

    private final MetaStatusCallbackSimulator callbackSimulator;
    private final ObjectMapper objectMapper;

    public SimulatedMetaClient(
            MetaStatusCallbackSimulator callbackSimulator, ObjectMapper objectMapper) {
        this.callbackSimulator = callbackSimulator;
        this.objectMapper = objectMapper;
        log.warn("Test profile active: Meta sends are SIMULATED. No message will reach WhatsApp.");
    }

    @Override
    public SendResponse send(
            String phoneNumberId, Long wabaAccountId, String accessToken, String requestPayload) {
        try {
            String wamid = generateWamid();
            String recipient = extractRecipient(requestPayload);
            String callbackData = extractCallbackData(requestPayload);

            MetaSendResponse envelope =
                    objectMapper.readValue(acceptedBody(wamid, recipient), MetaSendResponse.class);
            SendResponse response = MetaResponseMapper.toSendResponse(envelope);

            callbackSimulator.scheduleFor(
                    phoneNumberId, wabaAccountId, response.providerMessageId(), recipient,
                    callbackData);

            log.debug("Simulated send phoneNumberId={} to={} wamid={}",
                    phoneNumberId, recipient, response.providerMessageId());
            return response;

        } catch (Exception e) {
            // Matches the real client's contract: a send never throws, it returns a failure.
            log.error("Simulator failure phoneNumberId={}", phoneNumberId, e);
            return SendResponse.unreachable(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /** The body Meta returns on acceptance. */
    private String acceptedBody(String wamid, String recipient) {
        String contact = escape(recipient);
        return """
                {
                  "messaging_product": "whatsapp",
                  "contacts": [{"input": "%s", "wa_id": "%s"}],
                  "messages": [{"id": "%s", "message_status": "accepted"}]
                }
                """.formatted(contact, contact, wamid);
    }

    /**
     * Pulls {@code to} out of the request body.
     *
     * <p>The payload is rendered upstream and forwarded verbatim, so this is the only place the
     * recipient's number appears — and the status webhook needs it for {@code recipient_id}.
     */
    private String extractRecipient(String requestPayload) {
        if (requestPayload == null || requestPayload.isBlank()) {
            return null;
        }
        try {
            JsonNode to = objectMapper.readTree(requestPayload).get("to");
            return to == null || to.isNull() ? null : to.asText();
        } catch (Exception e) {
            log.debug("No readable 'to' in the request payload");
            return null;
        }
    }

    /**
     * Pulls {@code biz_opaque_callback_data} out of the request body so the status webhook can echo
     * it, exactly as Meta does.
     *
     * <p>Read as an opaque string and never parsed. Messaging Service correlates on
     * {@code msg:<messageId>}, but that prefix is theirs: interpreting it here would mean a change
     * to their correlation format needed a coordinated broadcast release. Absent until their
     * renderer sets it, and absent is a valid state rather than an error — the status simply carries
     * no callback data and the receiver falls back to the wamid.
     */
    private String extractCallbackData(String requestPayload) {
        if (requestPayload == null || requestPayload.isBlank()) {
            return null;
        }
        try {
            JsonNode data = objectMapper.readTree(requestPayload).get("biz_opaque_callback_data");
            return data == null || data.isNull() ? null : data.asText();
        } catch (Exception e) {
            log.debug("No readable 'biz_opaque_callback_data' in the request payload");
            return null;
        }
    }

    /**
     * Generates a message id shaped like a wamid but marked as simulated.
     *
     * <p>The prefix is deliberate. Simulated ids land in the same tables and topics as real ones, and
     * an id that cannot be told apart makes a polluted test database impossible to clean up.
     */
    private String generateWamid() {
        byte[] random = new byte[24];
        ThreadLocalRandom.current().nextBytes(random);
        return "wamid.SIM." + Base64.getUrlEncoder().withoutPadding().encodeToString(random);
    }

    /**
     * Escapes a value interpolated into the body template.
     *
     * <p>The recipient comes from a payload this service does not control, so it cannot be assumed
     * free of quotes. An unescaped one would produce a malformed body, and the parse failure would
     * surface as {@code unreachable} — a transport error, which retries — rather than as the bad
     * input it is.
     */
    private static String escape(String raw) {
        if (raw == null || raw.isEmpty()) {
            return "";
        }
        StringBuilder out = new StringBuilder(raw.length() + 8);
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }
}