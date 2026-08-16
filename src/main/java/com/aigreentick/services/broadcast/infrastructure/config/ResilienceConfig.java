package com.aigreentick.services.broadcast.infrastructure.config;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Circuit breakers, one instance per phone number, created on demand from this registry.
 *
 * <p>Per number rather than per service because Meta being unreachable for one number says nothing
 * about the others, and a single shared breaker would let one bad number stop every campaign on the
 * platform.
 *
 * <p>Used programmatically rather than through annotations: the breaker has to be selected by phone
 * number at call time, which an annotation cannot express.
 */
@Configuration
public class ResilienceConfig {

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry(
            BroadcastProperties properties, MeterRegistry meterRegistry) {

        BroadcastProperties.CircuitBreaker settings = properties.circuitBreaker();

        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(settings.failureRateThreshold())
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(settings.slidingWindowSize())
                .minimumNumberOfCalls(settings.minimumNumberOfCalls())
                .waitDurationInOpenState(settings.waitDurationInOpenState())
                .permittedNumberOfCallsInHalfOpenState(1)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                // Only transport failures count. Meta answering with a business error means Meta is
                // reachable and working; opening a breaker over invalid recipients would stop a
                // phone number that has nothing wrong with it.
                .recordExceptions(java.io.IOException.class, java.util.concurrent.TimeoutException.class)
                .build();

        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(config);
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry).bindTo(meterRegistry);
        return registry;
    }
}
