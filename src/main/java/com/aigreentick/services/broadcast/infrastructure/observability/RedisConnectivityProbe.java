package com.aigreentick.services.broadcast.infrastructure.observability;

import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Says once, at startup, whether Redis is actually reachable.
 *
 * <h2>Why this is needed at all</h2>
 * Lettuce connects lazily. Spring Data Redis builds the connection factory during context refresh
 * but opens no socket until a command is issued, and the only commands this service issues are on
 * the dispatch path. A service can therefore sit idle for hours, fully started, having never
 * discovered that the host in {@code spring.data.redis.host} does not answer — and find out for the
 * first time when the first campaign arrives and every send silently drops onto the local fallback.
 *
 * <p>That is the worst moment to learn it. The fallback limiter is per-instance, so what looks like
 * a working deployment is quietly over-sending by a factor of the replica count, which earns Meta
 * rate limits and lowers the number's quality rating — the expensive, slow-to-undo failure the
 * rate limiter exists to prevent.
 *
 * <h2>Why this does not fail startup</h2>
 * It reports; it does not block. Refusing to start without Redis would turn a cache outage into a
 * platform outage, and the fallback path exists precisely so the service can keep working without
 * it. The same reasoning that keeps the Redis health indicator on readiness rather than liveness
 * applies here.
 *
 * <p>One line, once, on a startup event. Nothing about this runs on the dispatch path.
 */
@Component
public class RedisConnectivityProbe {

    private static final Logger log = LoggerFactory.getLogger(RedisConnectivityProbe.class);

    private final StringRedisTemplate redis;
    private final BroadcastProperties properties;

    public RedisConnectivityProbe(StringRedisTemplate redis, BroadcastProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void probeOnStartup() {
        Instant started = Instant.now();

        try (RedisConnection connection = redis.getRequiredConnectionFactory().getConnection()) {
            String pong = connection.ping();
            long millis = Duration.between(started, Instant.now()).toMillis();

            log.info("Redis reachable: ping={} roundTripMs={} idempotencyEnabled={}",
                    pong, millis, properties.idempotency().enabled());

        } catch (Exception e) {
            // WARN and not ERROR: the service works without Redis, on conservative per-instance
            // rates. It is degraded, not broken, and an ERROR here would page someone for a
            // condition the fallback is designed to absorb.
            log.warn("Redis NOT reachable at startup; the token bucket and the idempotency guard "
                            + "will run on local fallback until it recovers. Per-instance rate "
                            + "limiting is not a global limit — check spring.data.redis.host. "
                            + "reason={}",
                    e.toString());
        }
    }
}
