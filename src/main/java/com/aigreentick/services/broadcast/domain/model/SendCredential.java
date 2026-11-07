package com.aigreentick.services.broadcast.domain.model;

/**
 * How one batch is sent, as the Messaging Service routed it (doc 47 R3): Meta Graph with the WABA's
 * token, or the WABA's BSP (e.g. Pinnacle) with the customer's key. This service never decides the
 * provider; it carries out what the batch says.
 *
 * <p>{@code provider} is a string, not an enum, so a new BSP needs no change here — only a new
 * {@code SendEndpoint}. A batch without a provider (from a Messaging Service older than R3, or in
 * flight during the deploy) is Meta, which is what every batch was before.
 *
 * <p>Both secrets arrive on the Kafka payload. See {@code DispatchEvent} for why that is a known
 * problem. {@link #toString()} redacts them.
 *
 * @param provider     {@value #META_DIRECT} or a BSP code such as {@code PINNACLE}
 * @param accessToken  the Meta token
 * @param bspApiKey    the BSP key; required when {@code provider} is a BSP
 * @param bspAccountId the BSP account id, for support only
 */
public record SendCredential(String provider, String accessToken, String bspApiKey, String bspAccountId) {

    public static final String META_DIRECT = "META_DIRECT";

    public SendCredential {
        provider = provider == null || provider.isBlank() ? META_DIRECT : provider;
    }

    public static SendCredential meta(String accessToken) {
        return new SendCredential(META_DIRECT, accessToken, null, null);
    }

    public boolean isMeta() {
        return META_DIRECT.equalsIgnoreCase(provider);
    }

    /** The secret the route needs: the token for Meta, the key for a BSP. */
    public boolean usable() {
        String secret = isMeta() ? accessToken : bspApiKey;
        return secret != null && !secret.isBlank();
    }

    @Override
    public String toString() {
        return "SendCredential[provider=" + provider
                + ", accessToken=" + (accessToken == null ? null : "***")
                + ", bspApiKey=" + (bspApiKey == null ? null : "***")
                + ", bspAccountId=" + bspAccountId + "]";
    }
}
