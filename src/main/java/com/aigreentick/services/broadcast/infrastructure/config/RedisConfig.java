package com.aigreentick.services.broadcast.infrastructure.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scripting.support.ResourceScriptSource;

import java.util.List;

/**
 * Redis access.
 *
 * <p>String template only. Values here are counters, rates and timestamps, and keeping them as plain
 * strings means the same keys can be read and corrected with {@code redis-cli} during an incident —
 * which is worth more than the small convenience of typed serialization.
 */
@Configuration
public class RedisConfig {

    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
    }

    /**
     * The token bucket, as a single Lua script.
     *
     * <p>Read-refill-take has to be one atomic step. Two instances performing it as separate
     * commands would both read the same token count and both spend it, which is exactly the
     * over-sending this service exists to prevent.
     */
    @Bean
    @SuppressWarnings("unchecked")
    public RedisScript<List> tokenBucketScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource("redis/token_bucket.lua")));
        script.setResultType(List.class);
        return script;
    }
}
