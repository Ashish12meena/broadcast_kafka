package com.aigreentick.services.broadcast.infrastructure.meta.simulator.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * The delivery status webhook Meta sends after a message is accepted.
 *
 * <p>Mirrors the real {@code whatsapp_business_account} envelope, carrying the fields a status
 * update is actually identified by. Nulls are omitted on serialization so an absent value produces
 * no key rather than an explicit null, as with the real webhook.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record MetaStatusWebhook(
        String object,
        List<Entry> entry) {

    /** {@code id} is the account the send belongs to, taken from the dispatch event. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Entry(
            String id,
            List<Change> changes) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Change(
            Value value,
            String field) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Value(
            @JsonProperty("messaging_product") String messagingProduct,
            Metadata metadata,
            List<Status> statuses) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Metadata(
            @JsonProperty("phone_number_id") String phoneNumberId) {
    }

    /**
     * @param id           the wamid returned by the send this status belongs to — the only link
     *                     between the two, which is why the simulator reports back the same value it
     *                     handed out
     * @param timestamp    seconds since epoch as a string, as Meta sends it: not milliseconds, and
     *                     not a number
     * @param recipientId  the recipient's number in international format without a plus
     * @param callbackData whatever the sender put in {@code biz_opaque_callback_data} on the send,
     *                     echoed back verbatim. Meta does not interpret this value and neither does
     *                     the simulator — Messaging Service's correlation format ({@code msg:<id>})
     *                     can therefore change without a coordinated release here. Absent until the
     *                     send payload carries it, since {@code NON_NULL} drops the key entirely
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Status(
            String id,
            String status,
            String timestamp,
            @JsonProperty("recipient_id") String recipientId,
            @JsonProperty("biz_opaque_callback_data") String callbackData) {
    }
}