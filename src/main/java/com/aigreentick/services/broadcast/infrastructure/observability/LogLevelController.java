package com.aigreentick.services.broadcast.infrastructure.observability;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runtime log level changes that expire on their own.
 *
 * <h2>Why the TTL is the important part</h2>
 * The failure mode of runtime log control is not the change, it is the change nobody reverts.
 * Someone raises a package to DEBUG at 2am to chase an incident, the incident ends, and three weeks
 * later the ingestion bill is the thing that notices. Every override here carries an expiry and
 * snaps back to whatever {@code application-{profile}.yml} declared.
 *
 * <p>Reverting means setting the level to {@code null}, not to INFO. Null restores inheritance, so
 * the baseline stays owned by configuration rather than being overwritten by a guess.
 *
 * <h2>Auditing</h2>
 * Every change logs at WARN, deliberately. When an incident review asks why this service was
 * emitting many times its normal volume for two hours, that line is the answer.
 */
@Component
public class LogLevelController {

    private static final Logger log = LoggerFactory.getLogger(LogLevelController.class);

    /** Ceiling on how long any single override can last, however long was requested. */
    public static final Duration MAX_TTL = Duration.ofHours(2);

    /** Applied when a caller supplies no TTL. Long enough to reproduce, short enough to forget. */
    public static final Duration DEFAULT_TTL = Duration.ofMinutes(15);

    private final LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
    private final Map<String, Instant> expiries = new ConcurrentHashMap<>();
    private final Map<String, Instant> campaignExpiries = new ConcurrentHashMap<>();
    private final Map<String, Instant> phoneNumberExpiries = new ConcurrentHashMap<>();

    /**
     * Applies an override to one logger.
     *
     * @param loggerName package or class name, e.g. {@code com.aigreentick.services.broadcast.infrastructure.redis}
     * @param level      TRACE/DEBUG/INFO/WARN/ERROR/OFF, or null to revert to the configured level
     * @param ttl        how long before automatic revert; capped at {@link #MAX_TTL}
     */
    public void apply(String loggerName, String level, Duration ttl) {
        ch.qos.logback.classic.Logger logger = context.getLogger(loggerName);

        if (level == null) {
            logger.setLevel(null);
            expiries.remove(loggerName);
            log.warn("Log level override cleared loggerName={}", loggerName);
            return;
        }

        Duration effectiveTtl = capped(ttl);
        logger.setLevel(Level.toLevel(level, Level.INFO));
        expiries.put(loggerName, Instant.now().plus(effectiveTtl));

        log.warn("Log level override applied loggerName={} level={} ttlSeconds={} expiresAt={}",
                loggerName, level, effectiveTtl.toSeconds(), expiries.get(loggerName));
    }

    /** Current overrides and when each one lapses. */
    public Map<String, Instant> activeOverrides() {
        return Map.copyOf(expiries);
    }

    /**
     * Turns on DEBUG for one campaign only, leaving every other campaign at its configured level.
     * Expires on the same schedule as a level override, so a forgotten campaign trace cannot outlive
     * the incident it was opened for.
     */
    public void debugCampaign(String campaignId, Duration ttl) {
        Duration effectiveTtl = capped(ttl);
        TargetedDebugFilter.debugCampaign(campaignId);
        campaignExpiries.put(campaignId, Instant.now().plus(effectiveTtl));
        log.warn("Targeted debug enabled campaignId={} ttlSeconds={}", campaignId, effectiveTtl.toSeconds());
    }

    /** As {@link #debugCampaign}, scoped to one phone number instead. */
    public void debugPhoneNumber(String phoneNumberId, Duration ttl) {
        Duration effectiveTtl = capped(ttl);
        TargetedDebugFilter.debugPhoneNumber(phoneNumberId);
        phoneNumberExpiries.put(phoneNumberId, Instant.now().plus(effectiveTtl));
        log.warn("Targeted debug enabled phoneNumberId={} ttlSeconds={}",
                phoneNumberId, effectiveTtl.toSeconds());
    }

    /**
     * Reverts anything past its expiry. Wired to the existing scheduler; a minute of granularity is
     * ample for something whose shortest useful lifetime is measured in minutes.
     */
    @Scheduled(fixedDelay = 60_000L)
    public void revertExpired() {
        Instant now = Instant.now();

        expiries.forEach((loggerName, expiry) -> {
            if (now.isAfter(expiry)) {
                apply(loggerName, null, Duration.ZERO);
            }
        });

        campaignExpiries.forEach((campaignId, expiry) -> {
            if (now.isAfter(expiry)) {
                TargetedDebugFilter.stopDebugCampaign(campaignId);
                campaignExpiries.remove(campaignId);
                log.warn("Targeted debug expired campaignId={}", campaignId);
            }
        });

        phoneNumberExpiries.forEach((phoneNumberId, expiry) -> {
            if (now.isAfter(expiry)) {
                TargetedDebugFilter.stopDebugPhoneNumber(phoneNumberId);
                phoneNumberExpiries.remove(phoneNumberId);
                log.warn("Targeted debug expired phoneNumberId={}", phoneNumberId);
            }
        });
    }

    private static Duration capped(Duration requested) {
        if (requested == null || requested.isZero() || requested.isNegative()) {
            return DEFAULT_TTL;
        }
        return requested.compareTo(MAX_TTL) > 0 ? MAX_TTL : requested;
    }
}
