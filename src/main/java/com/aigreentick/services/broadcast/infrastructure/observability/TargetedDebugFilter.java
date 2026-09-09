package com.aigreentick.services.broadcast.infrastructure.observability;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import com.aigreentick.services.broadcast.common.constants.ObservabilityConstants;
import org.slf4j.MDC;
import org.slf4j.Marker;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Verbose logging for one campaign or one phone number, with everything else left at INFO.
 *
 * <h2>The problem this solves</h2>
 * "Customer X says their broadcast did not go out" is the common ticket, and the usual response —
 * raise the whole service to DEBUG — multiplies fleet log volume by a large factor to obtain a few
 * hundred relevant lines. At 80 mps per number that is an expensive way to answer one question, and
 * on a shared log pipeline it degrades everyone else's queries at the same time.
 *
 * <p>The send path already puts {@code campaignId} and {@code phoneNumberId} in the MDC, so the
 * filter has everything it needs to decide per log event.
 *
 * <h2>Hot path cost</h2>
 * {@code decide} runs on every log invocation including suppressed ones, so the common case must be
 * a single volatile read and nothing else. That is what {@code active} is for: while no target is
 * registered the filter returns NEUTRAL before touching the MDC or either set.
 *
 * <p>State is static because Logback instantiates the filter itself from the XML configuration and
 * the Spring context has no reference to that instance.
 */
public class TargetedDebugFilter extends TurboFilter {

    private static final Set<String> DEBUG_CAMPAIGNS = ConcurrentHashMap.newKeySet();
    private static final Set<String> DEBUG_PHONE_IDS = ConcurrentHashMap.newKeySet();

    /** Single volatile read guarding the hot path. False whenever both sets are empty. */
    private static volatile boolean active = false;

    public static void debugCampaign(String campaignId) {
        DEBUG_CAMPAIGNS.add(campaignId);
        active = true;
    }

    public static void debugPhoneNumber(String phoneNumberId) {
        DEBUG_PHONE_IDS.add(phoneNumberId);
        active = true;
    }

    public static void stopDebugCampaign(String campaignId) {
        DEBUG_CAMPAIGNS.remove(campaignId);
        recomputeActive();
    }

    public static void stopDebugPhoneNumber(String phoneNumberId) {
        DEBUG_PHONE_IDS.remove(phoneNumberId);
        recomputeActive();
    }

    public static void clear() {
        DEBUG_CAMPAIGNS.clear();
        DEBUG_PHONE_IDS.clear();
        active = false;
    }

    public static Set<String> targetedCampaigns() {
        return Set.copyOf(DEBUG_CAMPAIGNS);
    }

    public static Set<String> targetedPhoneNumbers() {
        return Set.copyOf(DEBUG_PHONE_IDS);
    }

    private static void recomputeActive() {
        active = !DEBUG_CAMPAIGNS.isEmpty() || !DEBUG_PHONE_IDS.isEmpty();
    }

    @Override
    public FilterReply decide(
            Marker marker, Logger logger, Level level, String format, Object[] params, Throwable t) {

        // Fast path. Nothing targeted, or the event would be emitted anyway.
        if (!active || level.toInt() >= Level.INFO_INT) {
            return FilterReply.NEUTRAL;
        }

        String campaignId = MDC.get(ObservabilityConstants.Logging.MDC_CAMPAIGN_ID);
        if (campaignId != null && DEBUG_CAMPAIGNS.contains(campaignId)) {
            return FilterReply.ACCEPT;
        }

        String phoneNumberId = MDC.get(ObservabilityConstants.Logging.MDC_PHONE_NUMBER_ID);
        if (phoneNumberId != null && DEBUG_PHONE_IDS.contains(phoneNumberId)) {
            return FilterReply.ACCEPT;
        }

        // NEUTRAL rather than DENY: the configured level still decides, so this filter can only ever
        // add lines for a target, never remove lines someone asked for by other means.
        return FilterReply.NEUTRAL;
    }
}
