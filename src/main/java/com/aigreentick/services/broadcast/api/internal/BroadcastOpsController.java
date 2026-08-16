package com.aigreentick.services.broadcast.api.internal;

import com.aigreentick.services.broadcast.api.internal.dto.CapacityResponse;
import com.aigreentick.services.broadcast.api.internal.dto.DispatchStatsResponse;
import com.aigreentick.services.broadcast.application.service.capacity.CapacityService;
import com.aigreentick.services.broadcast.application.service.dispatch.DispatchScheduler;
import com.aigreentick.services.broadcast.application.service.ingest.ConsumerFlowController;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only operational endpoints.
 *
 * <p>Deliberately read-only. Anything that changes a phone number's rate belongs to the Messaging
 * Service, which owns the durable record — an override applied here would be silently reverted by
 * the next capacity event, which is worse than having no override at all.
 */
@RestController
@RequestMapping("/internal/broadcast")
public class BroadcastOpsController {

    private final DispatchScheduler scheduler;
    private final ConsumerFlowController flowController;
    private final CapacityService capacityService;

    public BroadcastOpsController(
            DispatchScheduler scheduler,
            ConsumerFlowController flowController,
            CapacityService capacityService) {
        this.scheduler = scheduler;
        this.flowController = flowController;
        this.capacityService = capacityService;
    }

    @GetMapping("/stats")
    public DispatchStatsResponse stats() {
        return new DispatchStatsResponse(
                scheduler.activeNumbers(),
                scheduler.totalPendingRecipients(),
                flowController.isPaused(),
                scheduler.isShuttingDown(),
                scheduler.depthByPhoneNumber());
    }

    @GetMapping("/capacity/{phoneNumberId}")
    public ResponseEntity<CapacityResponse> capacity(@PathVariable String phoneNumberId) {
        return capacityService.find(phoneNumberId)
                .map(capacity -> ResponseEntity.ok(new CapacityResponse(
                        capacity.phoneNumberId(),
                        capacity.configuredMps(),
                        capacity.effectiveMps(),
                        capacity.tier(),
                        capacity.backoffUntilMs(),
                        capacity.updatedAtMs(),
                        capacity.source().name())))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
