package com.aigreentick.services.broadcast.infrastructure.meta;

import io.netty.channel.ConnectTimeoutException;
import io.netty.handler.timeout.ReadTimeoutException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

/** Which transport failures are safe to retry: only those before the request left. */
class MetaCloudApiClientTransportTest {

    @Test
    void failuresBeforeTheRequestLeftAreSafeToRetry() {
        assertThat(MetaCloudApiClient.failedBeforeRequestWasSent(new ConnectException("refused"))).isTrue();
        assertThat(MetaCloudApiClient.failedBeforeRequestWasSent(new ConnectTimeoutException("slow"))).isTrue();
        assertThat(MetaCloudApiClient.failedBeforeRequestWasSent(
                new RuntimeException("wrapped", new UnknownHostException("graph.facebook.com")))).isTrue();
    }

    @Test
    void failuresAfterTheRequestLeftAreOutcomeUnknown() {
        assertThat(MetaCloudApiClient.failedBeforeRequestWasSent(ReadTimeoutException.INSTANCE)).isFalse();
        assertThat(MetaCloudApiClient.failedBeforeRequestWasSent(
                new RuntimeException("wrapped", new IOException("Connection reset by peer")))).isFalse();
    }
}
