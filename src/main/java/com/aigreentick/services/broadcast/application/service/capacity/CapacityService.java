package com.aigreentick.services.broadcast.application.service.capacity;

import com.aigreentick.services.broadcast.application.port.in.UpdateCapacityUseCase;
import com.aigreentick.services.broadcast.application.port.out.CapacityStorePort;
import com.aigreentick.services.broadcast.domain.model.CapacitySource;
import com.aigreentick.services.broadcast.domain.model.PhoneNumberCapacity;
import com.aigreentick.services.broadcast.infrastructure.config.BroadcastProperties;
import com.aigreentick.services.broadcast.infrastructure.observability.BroadcastMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Applies capacity published by the Messaging Service, and answers questions about it.
 *
 * <h2>This service does not discover capacity</h2>
 * It never calls Meta and never calls the WABA service. A phone number's throughput tier is owned by
 * the WABA service and the working rate derived from it is owned by the Messaging Service, which
 * already keeps both in {@code waba_throughput_state} with the degrade-and-recover loop around them.
 * A second discovery path here would produce a second answer for the same number, and the two would
 * disagree the first time either one missed an update.
 *
 * <p>What this service does own is the observation that a number is currently being rate-limited,
 * which only the sender can see. That flows the other way, through the result stream.
 */
@Service
public class CapacityService implements UpdateCapacityUseCase {

    private static final Logger log = LoggerFactory.getLogger(CapacityService.class);

    private final CapacityStorePort capacityStore;
    private final BroadcastProperties properties;
    private final BroadcastMetrics metrics;

    public CapacityService(
            CapacityStorePort capacityStore, BroadcastProperties properties, BroadcastMetrics metrics) {
        this.capacityStore = capacityStore;
        this.properties = properties;
        this.metrics = metrics;
    }

    @Override
    public void apply(PhoneNumberCapacity capacity) {
        capacityStore.put(capacity);
        metrics.capacity(
                capacity.phoneNumberId(),
                capacity.effectiveMps(),
                capacity.configuredMps(),
                capacity.source());
    }

    public Optional<PhoneNumberCapacity> find(String phoneNumberId) {
        return capacityStore.find(phoneNumberId);
    }

    /**
     * What to assume about a number nothing is known about.
     *
     * <p>The configured default, never the high tier. Guessing low delays a campaign; guessing high
     * earns rate limits, and sustained rate limiting lowers the number's quality rating, which
     * lowers the real ceiling — a slow and expensive thing to undo.
     */
    public PhoneNumberCapacity defaultCapacity(String phoneNumberId) {
        int defaultMps = properties.rateLimit().defaultMps();
        log.debug("Using default capacity for phoneNumberId={} mps={}", phoneNumberId, defaultMps);
        return new PhoneNumberCapacity(
                phoneNumberId, defaultMps, defaultMps, "UNKNOWN", 0,
                System.currentTimeMillis(), CapacitySource.DEFAULT);
    }
}
