package com.aigreentick.services.broadcast.infrastructure.kafka.event;

import com.aigreentick.services.broadcast.domain.model.SendCredential;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The route on the event (doc 47 R3): old producers still read as Meta, and secrets never print. */
class DispatchEventCredentialTest {

    private final ObjectMapper json = new ObjectMapper();

    @Test
    void anEventFromAProducerOlderThanR3IsMeta() throws Exception {
        DispatchEvent event = json.readValue("""
                {"campaignId":1,"phoneNumberId":"pn-17","wabaAccountId":5,"accessToken":"meta-token",
                 "traceId":"T1","payloads":[]}""", DispatchEvent.class);

        SendCredential credential = event.credential();

        assertThat(credential.isMeta()).isTrue();
        assertThat(credential.provider()).isEqualTo(SendCredential.META_DIRECT);
        assertThat(credential.accessToken()).isEqualTo("meta-token");
        assertThat(credential.usable()).isTrue();
    }

    @Test
    void aPinnacleEventCarriesItsRouteAndKey() throws Exception {
        DispatchEvent event = json.readValue("""
                {"campaignId":1,"phoneNumberId":"pn-174","wabaAccountId":21,"accessToken":"meta-token",
                 "traceId":"T1","payloads":[],"provider":"PINNACLE","bspApiKey":"pin-key","bspAccountId":"p-1"}""",
                DispatchEvent.class);

        SendCredential credential = event.credential();

        assertThat(credential.provider()).isEqualTo("PINNACLE");
        assertThat(credential.isMeta()).isFalse();
        assertThat(credential.bspApiKey()).isEqualTo("pin-key");
        assertThat(credential.usable()).isTrue();
    }

    @Test
    void aPinnacleEventWithoutAKeyIsNotUsable() throws Exception {
        DispatchEvent event = json.readValue("""
                {"campaignId":1,"phoneNumberId":"pn-174","accessToken":"meta-token","payloads":[],
                 "provider":"PINNACLE"}""", DispatchEvent.class);

        assertThat(event.credential().usable()).isFalse();
    }

    @Test
    void neitherTheEventNorTheCredentialPrintsASecret() throws Exception {
        DispatchEvent event = json.readValue("""
                {"campaignId":1,"phoneNumberId":"pn-174","accessToken":"meta-token","payloads":[],
                 "provider":"PINNACLE","bspApiKey":"pin-key"}""", DispatchEvent.class);

        assertThat(event.toString()).doesNotContain("meta-token").doesNotContain("pin-key").contains("PINNACLE");
        assertThat(event.credential().toString()).doesNotContain("meta-token").doesNotContain("pin-key");
    }
}
