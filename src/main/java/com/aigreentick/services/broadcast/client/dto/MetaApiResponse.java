package com.aigreentick.services.broadcast.client.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

/**
 * Response from Meta's POST /{phoneNumberId}/messages endpoint.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetaApiResponse {

    @JsonProperty("messaging_product")
    private String messagingProduct;

    private List<ContactDto> contacts;
    private List<MessageDto> messages;

    // Populated on error responses
    private ErrorDto error;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ContactDto {
        private String input;

        @JsonProperty("wa_id")
        private String waId;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MessageDto {
        private String id;  // wamid.xxx — provider message ID

        @JsonProperty("message_status")
        private String messageStatus;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ErrorDto {
        private int code;
        private String message;
        private String type;

        @JsonProperty("fbtrace_id")
        private String fbtraceId;
    }

    public boolean isSuccess() {
        return error == null && messages != null && !messages.isEmpty();
    }

    public String getProviderMessageId() {
        if (messages != null && !messages.isEmpty()) {
            return messages.get(0).getId();
        }
        return null;
    }
}