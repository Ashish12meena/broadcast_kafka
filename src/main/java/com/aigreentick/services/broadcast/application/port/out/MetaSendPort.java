package com.aigreentick.services.broadcast.application.port.out;

import com.aigreentick.services.broadcast.domain.model.SendResponse;

/**
 * Sends one message to Meta.
 *
 * <p>Implementations never throw. A failed send is a returned {@link SendResponse}, so one bad
 * recipient cannot abort the others being sent alongside it.
 *
 * <p>{@code wabaAccountId} is not used when talking to Meta — the Graph call is addressed by
 * {@code phoneNumberId} alone. It is carried because it identifies which account the send belongs
 * to, which the test-profile simulator needs in order to report a status back against the right one.
 */
public interface MetaSendPort {

    SendResponse send(
            String phoneNumberId, Long wabaAccountId, String accessToken, String requestPayload);
}
