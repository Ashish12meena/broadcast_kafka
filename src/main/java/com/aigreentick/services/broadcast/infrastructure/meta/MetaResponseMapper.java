package com.aigreentick.services.broadcast.infrastructure.meta;

import com.aigreentick.services.broadcast.domain.model.SendResponse;
import com.aigreentick.services.broadcast.infrastructure.meta.dto.MetaSendResponse;

/**
 * Turns Meta's response envelope into a {@link SendResponse}.
 *
 * <p>Extracted from {@link MetaCloudApiClient} so the simulator used under the test profile can
 * reuse it verbatim rather than reimplementing it. That reuse is the point: a simulator that builds
 * its own {@code SendResponse} would drift from the real mapping the first time either side changed,
 * and the drift would show up as a test suite that passes against behaviour production does not
 * have. Here there is one mapping and both callers go through it.
 */
public final class MetaResponseMapper {

    /** Stands in for a response with no body, so the caller sees a value rather than a null. */
    public static final MetaSendResponse EMPTY_RESPONSE = new MetaSendResponse(null, null, null);

    private MetaResponseMapper() {
    }

    /**
     * @param response the deserialized Meta envelope, or null if nothing came back
     */
    public static SendResponse toSendResponse(MetaSendResponse response) {
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
    }
}
