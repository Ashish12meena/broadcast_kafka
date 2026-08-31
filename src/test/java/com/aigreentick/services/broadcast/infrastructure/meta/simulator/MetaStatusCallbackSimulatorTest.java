package com.aigreentick.services.broadcast.infrastructure.meta.simulator;

import com.aigreentick.services.broadcast.infrastructure.meta.simulator.dto.MetaStatusWebhook;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Covers the payload the simulator reports back and the delays it schedules statuses on. */
class MetaStatusCallbackSimulatorTest {

    private final MetaSimulatorProperties properties =
            new MetaSimulatorProperties("https://example.test/webhook", null, null, null);
    private final MetaStatusCallbackSimulator simulator =
            new MetaStatusCallbackSimulator(null, properties);

    // ------------------------------------------------------------- settings

    @Test
    void defaultsAreUsableWithOnlyAUrlSupplied() {
        assertEquals(Duration.ofSeconds(1), properties.minDelay());
        assertEquals(Duration.ofSeconds(8), properties.maxDelay());
        assertTrue(properties.headers().isEmpty());
        assertTrue(properties.callbacksEnabled());
    }

    @Test
    void callbacksAreDisabledWithoutAUrl() {
        assertFalse(new MetaSimulatorProperties(null, null, null, null).callbacksEnabled());
        assertFalse(new MetaSimulatorProperties("   ", null, null, null).callbacksEnabled());
    }

    @Test
    void rejectsAMaxDelayBelowTheMinimum() {
        assertThrows(IllegalArgumentException.class, () -> new MetaSimulatorProperties(
                "https://example.test/webhook", null, Duration.ofSeconds(9), Duration.ofSeconds(2)));
    }

    // --------------------------------------------------------------- delays

    @Test
    void delaysStayWithinTheConfiguredBandAndVary() {
        Duration min = Duration.ofSeconds(1);
        Duration max = Duration.ofSeconds(8);
        long changes = 0;
        long previous = -1;

        for (int i = 0; i < 20_000; i++) {
            long ms = MetaStatusCallbackSimulator.randomDelay(min, max).toMillis();
            assertTrue(ms >= 1_000, "delay below configured minimum: " + ms);
            assertTrue(ms <= 8_000, "delay above configured maximum: " + ms);
            if (ms != previous) {
                changes++;
                previous = ms;
            }
        }
        assertTrue(changes > 100, "delay does not appear to be random");
    }

    @Test
    void anEqualBandYieldsAFixedDelay() {
        Duration two = Duration.ofSeconds(2);
        for (int i = 0; i < 500; i++) {
            assertEquals(2_000L, MetaStatusCallbackSimulator.randomDelay(two, two).toMillis());
        }
    }

    @Test
    void cumulativeDelaysKeepStatusesInOrder() {
        Duration min = Duration.ofSeconds(1);
        Duration max = Duration.ofSeconds(8);

        for (int i = 0; i < 5_000; i++) {
            Duration cumulative = Duration.ZERO;
            long previous = -1;
            for (int status = 0; status < 3; status++) {
                cumulative = cumulative.plus(MetaStatusCallbackSimulator.randomDelay(min, max));
                assertTrue(cumulative.toMillis() > previous,
                        "a status was scheduled no later than the one before it");
                previous = cumulative.toMillis();
            }
        }
    }

    // ----------------------------------------------------------- recipients

    @Test
    void recipientIsReducedToDigits() {
        assertEquals("919876543210", MetaStatusCallbackSimulator.normalisePhone("+919876543210"));
        assertEquals("919876543210", MetaStatusCallbackSimulator.normalisePhone("+91 98765-43210"));
        assertEquals("919876543210", MetaStatusCallbackSimulator.normalisePhone("919876543210"));
    }

    @Test
    void anAbsentRecipientFallsBackRatherThanFailing() {
        assertEquals("0000000000", MetaStatusCallbackSimulator.normalisePhone(null));
        assertEquals("0000000000", MetaStatusCallbackSimulator.normalisePhone("   "));
    }

    // -------------------------------------------------------------- payload

    @Test
    void envelopeMatchesMetasShape() {
        MetaStatusWebhook webhook = build("sent");

        assertEquals("whatsapp_business_account", webhook.object());
        assertEquals("messages", webhook.entry().get(0).changes().get(0).field());
        assertEquals("whatsapp",
                webhook.entry().get(0).changes().get(0).value().messagingProduct());
        assertEquals("123456789012345",
                webhook.entry().get(0).changes().get(0).value().metadata().phoneNumberId());
    }

    @Test
    void identifiersComeFromTheDispatchEventNotConfiguration() {
        MetaStatusWebhook webhook = build("sent");

        assertEquals("42", webhook.entry().get(0).id());
        assertEquals("123456789012345",
                webhook.entry().get(0).changes().get(0).value().metadata().phoneNumberId());
    }

    @Test
    void aMissingAccountIdIsOmittedRatherThanPrintedAsNull() {
        MetaStatusWebhook webhook = simulator.buildPayload(
                "123456789012345", null, "wamid.SIM.test", "919876543210", "sent", null);

        assertNull(webhook.entry().get(0).id());
    }

    @Test
    void statusCarriesTheWamidHandedBackBySend() {
        assertEquals("wamid.SIM.test", statusOf(build("delivered")).id());
        assertEquals("919876543210", statusOf(build("delivered")).recipientId());
        assertEquals("delivered", statusOf(build("delivered")).status());
    }

    @Test
    void statusEchoesTheCallbackDataFromTheSendRequest() {
        // The field the Messaging Service matches on before it matches on the wamid. Dropping it
        // here would leave the simulated round trip passing while the real one still races.
        assertEquals("msg:4242", statusOf(build("sent")).callbackData());
    }

    @Test
    void aStatusWithNoCallbackDataOmitsTheKeyRatherThanSendingNull() {
        assertNull(statusOf(simulator.buildPayload(
                "123456789012345", 42L, "wamid.SIM.test", "919876543210", "sent", null))
                .callbackData());
    }

    @Test
    void timestampIsEpochSecondsAsAString() {
        long seconds = Long.parseLong(statusOf(build("sent")).timestamp());

        assertTrue(seconds > 1_700_000_000L && seconds < 4_000_000_000L,
                "timestamp is not epoch seconds");
    }

    // --------------------------------------------------------------- helpers

    private MetaStatusWebhook build(String status) {
        return simulator.buildPayload(
                "123456789012345", 42L, "wamid.SIM.test", "919876543210", status, "msg:4242");
    }

    private static MetaStatusWebhook.Status statusOf(MetaStatusWebhook webhook) {
        return webhook.entry().get(0).changes().get(0).value().statuses().get(0);
    }
}