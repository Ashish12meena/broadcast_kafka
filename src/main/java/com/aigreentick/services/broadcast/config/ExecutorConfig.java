package com.aigreentick.services.broadcast.config;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;


import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableScheduling
@Slf4j
public class ExecutorConfig {

    @Value("${campaign.max-concurrent-users:100}")
    private int maxConcurrentUsers;

    @Value("${campaign.semaphore-cleanup-enabled:true}")
    private boolean semaphoreCleanupEnabled;
    
    @Value("${campaign.executor.core-pool-size:500}")
    private int executorCorePoolSize;
    
    @Value("${campaign.executor.max-pool-size:2000}")
    private int executorMaxPoolSize;
    
    @Value("${campaign.executor.queue-capacity:10000}")
    private int executorQueueCapacity;

    private final ConcurrentHashMap<String, Integer> activeCampaignsPerUser = new ConcurrentHashMap<>();
    
    // NEW: Track executor metrics
    private final AtomicLong totalTasksSubmitted = new AtomicLong(0);
    private final AtomicLong totalTasksCompleted = new AtomicLong(0);
    private final AtomicLong totalTasksRejected = new AtomicLong(0);

    @Bean(name = "broadcastExecutor", destroyMethod = "shutdown")
    public ExecutorService broadcastExecutor() {
        int poolSize = Math.max(100, maxConcurrentUsers);
        
        log.info("Initializing broadcastExecutor with pool size: {}", poolSize);
        
        return Executors.newFixedThreadPool(
                poolSize,
                r -> {
                    Thread t = new Thread(r);
                    t.setName("broadcast-" + t.getId());
                    t.setDaemon(false);
                    return t;
                });
    }

    /**
     * OPTIMIZED: High-performance executor for WhatsApp API calls
     * - Core pool: 500 threads (always alive)
     * - Max pool: 2000 threads (scales up under load)
     * - Queue: 10000 capacity (buffer for burst traffic)
     */
    @Bean(name = "whatsappExecutor", destroyMethod = "shutdown")
    public ExecutorService whatsappExecutor() {
        log.info("Initializing whatsappExecutor:");
        log.info("  - Core pool size: {} threads", executorCorePoolSize);
        log.info("  - Max pool size: {} threads", executorMaxPoolSize);
        log.info("  - Queue capacity: {} tasks", executorQueueCapacity);
        
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                executorCorePoolSize,
                executorMaxPoolSize,
                60L, // Keep alive time for idle threads
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(executorQueueCapacity),
                r -> {
                    Thread t = new Thread(r);
                    t.setName("whatsapp-" + t.getId());
                    t.setDaemon(false);
                    return t;
                },
                // Rejection policy: CallerRuns (blocks producer if queue full)
                new ThreadPoolExecutor.CallerRunsPolicy() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                        totalTasksRejected.incrementAndGet();
                        log.warn("WhatsApp executor queue full! Rejected task. Queue: {}/{}, Active: {}/{}",
                                e.getQueue().size(), executorQueueCapacity,
                                e.getActiveCount(), e.getMaximumPoolSize());
                        super.rejectedExecution(r, e);
                    }
                }
        );
        
        // Allow core threads to timeout
        executor.allowCoreThreadTimeOut(true);
        
        return executor;
    }

    @Bean(name = "maintenanceExecutor", destroyMethod = "shutdown")
    public ScheduledExecutorService maintenanceExecutor() {
        return Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r);
            t.setName("maintenance-" + t.getId());
            t.setDaemon(true);
            return t;
        });
    }

    @Bean
    public ConcurrentHashMap<String, Semaphore> userSemaphores() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public ConcurrentHashMap<String, Long> semaphoreLastUsed() {
        return new ConcurrentHashMap<>();
    }

    /**
     * Helper method to get or create semaphore with campaign tracking
     */
    public static Semaphore getSemaphoreForUser(
            ConcurrentHashMap<String, Semaphore> userSemaphores,
            ConcurrentHashMap<String, Long> semaphoreLastUsed,
            String phoneNumberId) {
        
        semaphoreLastUsed.put(phoneNumberId, System.currentTimeMillis());
        
        return userSemaphores.computeIfAbsent(
                phoneNumberId,
                key -> {
                    Semaphore semaphore = new Semaphore(ConfigConstants.MAX_CONCURRENT_WHATSAPP_REQUESTS);
                    log.info("Created new semaphore for user: {} (Total semaphores: {})", 
                            phoneNumberId, userSemaphores.size());
                    return semaphore;
                });
    }

    public void incrementActiveCampaigns(String phoneNumberId) {
        activeCampaignsPerUser.compute(phoneNumberId, (k, v) -> v == null ? 1 : v + 1);
    }

    public void decrementActiveCampaigns(String phoneNumberId) {
        activeCampaignsPerUser.compute(phoneNumberId, (k, v) -> {
            if (v == null || v <= 1) {
                return null;
            }
            return v - 1;
        });
    }

    @Scheduled(fixedRate = 3600000) // Every hour
    public void cleanupInactiveSemaphores() {
        if (!semaphoreCleanupEnabled) {
            return;
        }

        ConcurrentHashMap<String, Semaphore> semaphores = userSemaphores();
        ConcurrentHashMap<String, Long> lastUsed = semaphoreLastUsed();
        
        long now = System.currentTimeMillis();
        long inactivityThreshold = TimeUnit.HOURS.toMillis(6);
        
        int initialSize = semaphores.size();
        int removed = 0;
        
        for (var entry : semaphores.entrySet()) {
            String phoneNumberId = entry.getKey();
            Semaphore semaphore = entry.getValue();
            
            Integer activeCampaigns = activeCampaignsPerUser.get(phoneNumberId);
            if (activeCampaigns != null && activeCampaigns > 0) {
                continue;
            }
            
            if (semaphore.availablePermits() == ConfigConstants.MAX_CONCURRENT_WHATSAPP_REQUESTS) {
                Long lastUsedTime = lastUsed.get(phoneNumberId);
                
                if (lastUsedTime != null && (now - lastUsedTime) > inactivityThreshold) {
                    if (activeCampaignsPerUser.get(phoneNumberId) == null) {
                        if (semaphores.remove(phoneNumberId) != null) {
                            lastUsed.remove(phoneNumberId);
                            removed++;
                        }
                    }
                }
            }
        }
        
        if (removed > 0) {
            log.info("Semaphore cleanup completed. Removed: {}, Remaining: {} (was: {})", 
                    removed, semaphores.size(), initialSize);
        }
    }

    public SemaphoreStats getSemaphoreStats() {
        ConcurrentHashMap<String, Semaphore> semaphores = userSemaphores();
        
        int totalSemaphores = semaphores.size();
        int activeSemaphores = 0;
        int totalPermitsInUse = 0;
        int usersWithActiveCampaigns = activeCampaignsPerUser.size();
        
        for (Semaphore semaphore : semaphores.values()) {
            int inUse = ConfigConstants.MAX_CONCURRENT_WHATSAPP_REQUESTS - semaphore.availablePermits();
            if (inUse > 0) {
                activeSemaphores++;
                totalPermitsInUse += inUse;
            }
        }
        
        return new SemaphoreStats(
                totalSemaphores,
                activeSemaphores,
                totalPermitsInUse,
                totalSemaphores * ConfigConstants.MAX_CONCURRENT_WHATSAPP_REQUESTS,
                usersWithActiveCampaigns
        );
    }

    public record SemaphoreStats(
            int totalSemaphores,
            int activeSemaphores,
            int totalPermitsInUse,
            int totalPermitsAvailable,
            int usersWithActiveCampaigns
    ) {
        public double utilizationPercentage() {
            return totalPermitsAvailable > 0 
                    ? (totalPermitsInUse * 100.0) / totalPermitsAvailable 
                    : 0.0;
        }
    }
}

