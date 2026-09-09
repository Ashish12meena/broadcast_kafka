package com.aigreentick.services.broadcast.api.internal;

import com.aigreentick.services.broadcast.api.internal.dto.CapacityResponse;
import com.aigreentick.services.broadcast.common.constants.APIPaths;
import com.aigreentick.services.broadcast.api.internal.dto.DispatchStatsResponse;
import com.aigreentick.services.broadcast.application.service.capacity.CapacityService;
import com.aigreentick.services.broadcast.application.service.dispatch.DispatchScheduler;
import com.aigreentick.services.broadcast.application.service.ingest.ConsumerFlowController;
import com.aigreentick.services.broadcast.infrastructure.observability.LogLevelController;
import com.aigreentick.services.broadcast.infrastructure.observability.TargetedDebugFilter;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.Map;

/**
 * Read-only operational endpoints.
 *
 * <p>Read-only as far as dispatch behaviour is concerned. Anything that changes a phone number's
 * rate belongs to the Messaging Service, which owns the durable record — an override applied here
 * would be silently reverted by the next capacity event, which is worse than having no override at
 * all.
 *
 * <h2>The log control endpoints are the one exception</h2>
 * They change observability, never behaviour, and they expire on their own. The alternative to
 * having them is redeploying with a different log level to investigate a live incident, which
 * restarts the pods and destroys the state you were trying to look at.
 *
 * <p>These must not be reachable from outside the cluster. An unauthenticated level endpoint is a
 * denial-of-service vector: one request setting the service to TRACE is enough to drown the dispatch
 * loop. Keep this controller behind the internal management port and network policy.
 */
@RestController
@RequestMapping(APIPaths.INTERNAL_BROADCAST_BASE)
public class BroadcastOpsController {

    private final DispatchScheduler scheduler;
    private final ConsumerFlowController flowController;
    private final CapacityService capacityService;
    private final LogLevelController logLevelController;

    public BroadcastOpsController(
            DispatchScheduler scheduler,
            ConsumerFlowController flowController,
            CapacityService capacityService,
            LogLevelController logLevelController) {
        this.scheduler = scheduler;
        this.flowController = flowController;
        this.capacityService = capacityService;
        this.logLevelController = logLevelController;
    }

    @GetMapping(APIPaths.STATS)
    public DispatchStatsResponse stats() {
        return new DispatchStatsResponse(
                scheduler.activeNumbers(),
                scheduler.totalPendingRecipients(),
                flowController.isPaused(),
                scheduler.isShuttingDown(),
                scheduler.depthByPhoneNumber());
    }

    @GetMapping(APIPaths.CAPACITY_BY_PHONE_NUMBER)
    public ResponseEntity<CapacityResponse> capacity(
            @PathVariable(APIPaths.PATH_VAR_PHONE_NUMBER_ID) String phoneNumberId) {
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

    // ------------------------------------------------------------------ log control

    /**
     * What is currently overridden on this instance, and until when.
     *
     * <p>Worth checking first when prod behaves differently from the manifest: an override someone
     * forgot is the usual explanation.
     */
    @GetMapping(APIPaths.LOG_CONTROL)
    public Map<String, Object> loggingState() {
        return Map.of(
                "levelOverrides", logLevelController.activeOverrides(),
                "debugCampaigns", TargetedDebugFilter.targetedCampaigns(),
                "debugPhoneNumbers", TargetedDebugFilter.targetedPhoneNumbers());
    }

    /**
     * Verbose logging for one campaign. This is the endpoint to reach for on a "customer says their
     * broadcast did not go out" ticket — it yields the few hundred lines that matter without raising
     * fleet volume for every other campaign running at the same time.
     */
    @PostMapping(APIPaths.LOG_DEBUG_CAMPAIGN)
    public ResponseEntity<Void> debugCampaign(
            @PathVariable(APIPaths.PATH_VAR_CAMPAIGN_ID) String campaignId,
            @RequestParam(required = false) Integer ttlMinutes) {

        logLevelController.debugCampaign(campaignId, minutes(ttlMinutes));
        return ResponseEntity.accepted().build();
    }

    @DeleteMapping(APIPaths.LOG_DEBUG_CAMPAIGN)
    public ResponseEntity<Void> stopDebugCampaign(
            @PathVariable(APIPaths.PATH_VAR_CAMPAIGN_ID) String campaignId) {
        TargetedDebugFilter.stopDebugCampaign(campaignId);
        return ResponseEntity.noContent().build();
    }

    @PostMapping(APIPaths.LOG_DEBUG_PHONE_NUMBER)
    public ResponseEntity<Void> debugPhoneNumber(
            @PathVariable(APIPaths.PATH_VAR_PHONE_NUMBER_ID) String phoneNumberId,
            @RequestParam(required = false) Integer ttlMinutes) {

        logLevelController.debugPhoneNumber(phoneNumberId, minutes(ttlMinutes));
        return ResponseEntity.accepted().build();
    }

    @DeleteMapping(APIPaths.LOG_DEBUG_PHONE_NUMBER)
    public ResponseEntity<Void> stopDebugPhoneNumber(
            @PathVariable(APIPaths.PATH_VAR_PHONE_NUMBER_ID) String phoneNumberId) {
        TargetedDebugFilter.stopDebugPhoneNumber(phoneNumberId);
        return ResponseEntity.noContent().build();
    }

    private static Duration minutes(Integer value) {
        return value == null ? null : Duration.ofMinutes(value);
    }
}
