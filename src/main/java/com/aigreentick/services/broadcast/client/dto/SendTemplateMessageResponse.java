package com.aigreentick.services.broadcast.client.dto;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class SendTemplateMessageResponse {

    @JsonProperty("messaging_product")
    private String messagingProduct;

    private List<WhatsAppContactDto> contacts;
    private List<WhatsAppMessageDto> messages;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class WhatsAppContactDto {
        private String input;

        @JsonProperty("wa_id")
        private String waId;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class WhatsAppMessageDto {
        private String id;

        @JsonProperty("message_status")
        private String messageStatus;

        public WhatsAppMessageDto(String string, String string2) {
            this.id = string;
            this.messageStatus = string2;
        }

    }
}
