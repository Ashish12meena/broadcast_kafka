package com.aigreentick.services.broadcast.infrastructure.redis;

import com.aigreentick.services.broadcast.domain.model.PhoneNumberCapacity;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The last capacity this instance saw for each phone number, held in memory.
 *
 * <p>Exists for one purpose: when Redis is unreachable, the fallback limiter needs some idea of what
 * a number can do. Falling back to the global default would throttle a 1,000 mps number to 80, and
 * an outage should not cost that much throughput.
 *
 * <p>Bounded by the number of phone numbers this instance has handled, which is small and stable.
 */
@Component
public class CapacityMemory {

    private final Map<String, PhoneNumberCapacity> lastKnown = new ConcurrentHashMap<>();

    public void remember(PhoneNumberCapacity capacity) {
        if (capacity != null && capacity.phoneNumberId() != null) {
            lastKnown.put(capacity.phoneNumberId(), capacity);
        }
    }

    public Optional<PhoneNumberCapacity> lastKnown(String phoneNumberId) {
        return Optional.ofNullable(lastKnown.get(phoneNumberId));
    }

    public Map<String, PhoneNumberCapacity> snapshot() {
        return Map.copyOf(lastKnown);
    }
}
