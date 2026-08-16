package com.aigreentick.services.broadcast.infrastructure.observability;

import com.aigreentick.services.broadcast.domain.model.CapacitySource;
import com.aigreentick.services.broadcast.domain.model.PhoneNumberCapacity;
import com.aigreentick.services.broadcast.infrastructure.redis.CapacityMemory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Reports whether the service is enforcing the shared limit or improvising.
 *
 * <p>Deliberately registered as a health *detail* rather than something that fails readiness.
 * Running on local fallback is a degraded state that must be visible and alerted on, but a pod in
 * that state is still sending correctly and conservatively — removing it from the load balancer
 * would turn a Redis problem into a capacity problem.
 */
@Component
public class CapacityHealthIndicator implements HealthIndicator {

    private final CapacityMemory memory;

    public CapacityHealthIndicator(CapacityMemory memory) {
        this.memory = memory;
    }

    @Override
    public Health health() {
        Map<String, PhoneNumberCapacity> snapshot = memory.snapshot();

        long degraded = snapshot.values().stream()
                .filter(capacity -> capacity.effectiveMps() < capacity.configuredMps())
                .count();

        long onFallback = snapshot.values().stream()
                .filter(capacity -> capacity.source() == CapacitySource.LOCAL_FALLBACK)
                .count();

        return Health.up()
                .withDetail("knownPhoneNumbers", snapshot.size())
                .withDetail("degradedPhoneNumbers", degraded)
                .withDetail("phoneNumbersOnLocalFallback", onFallback)
                .build();
    }
}
