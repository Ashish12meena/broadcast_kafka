package com.aigreentick.services.broadcast.application.port.out;

import com.aigreentick.services.broadcast.domain.model.SendResponse;

/**
 * Sends one message to Meta.
 *
 * <p>Implementations never throw. A failed send is a returned {@link SendResponse}, so one bad
 * recipient cannot abort the others being sent alongside it.
 */
public interface MetaSendPort {

    SendResponse send(String phoneNumberId, String accessToken, String requestPayload);
}
