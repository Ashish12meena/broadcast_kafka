package com.aigreentick.services.broadcast.infrastructure.meta.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Meta's response to a send, success or failure.
 *
 * <p>One type for both because Meta returns the same envelope either way, distinguished by which
 * fields are populated. Unknown fields are ignored so a Graph version bump that adds a field cannot
 * break sending.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MetaSendResponse(
        @JsonProperty("messaging_product") String messagingProduct,
        List<SentMessage> messages,
        MetaError error) {

    /** The wamid, which is the only link between this send and every later delivery webhook. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SentMessage(
            String id,
            @JsonProperty("message_status") String messageStatus) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record MetaError(
            Integer code,
            String message,
            String type,
            @JsonProperty("error_subcode") Integer subcode,
            @JsonProperty("fbtrace_id") String traceId) {
    }

    public boolean accepted() {
        return error == null && messages != null && !messages.isEmpty();
    }

    public String providerMessageId() {
        return accepted() ? messages.get(0).id() : null;
    }

    public String messageStatus() {
        return accepted() ? messages.get(0).messageStatus() : null;
    }
}
