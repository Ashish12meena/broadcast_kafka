package com.aigreentick.services.broadcast.infrastructure.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread pools.
 *
 * <p>Sends and queue workers run on virtual threads. A send spends nearly all its time waiting on
 * Meta, and one platform thread per waiting request is the arrangement that makes a thousand
 * messages per second expensive — two thousand platform threads reserve roughly two gigabytes of
 * stack and keep the scheduler busy doing nothing. Virtual threads let the blocking code stay
 * blocking and readable while costing almost nothing to park.
 *
 * <p>In-flight work is bounded by a semaphore in the dispatch worker and by the HTTP connection
 * pool, not by the size of a thread pool. That is deliberate: the connection pool is in the request
 * path and cannot be forgotten, whereas a limit expressed as a pool size is invisible at the point
 * where it matters.
 */
@Configuration
public class ExecutorConfig {

    /** Runs one drain loop per active phone number, plus the individual sends they submit. */
    @Bean(name = "dispatchExecutor", destroyMethod = "close")
    public ExecutorService dispatchExecutor() {
        return Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("dispatch-", 0).factory());
    }

    /**
     * Timed work: retry re-queues and result flushes.
     *
     * <p>Platform threads, not virtual: scheduled executors hold their threads for the lifetime of
     * the service, and there are only a handful of them.
     */
    @Bean(name = "schedulerExecutor", destroyMethod = "shutdownNow")
    public ScheduledExecutorService schedulerExecutor() {
        AtomicLong counter = new AtomicLong();
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "broadcast-sched-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        return Executors.newScheduledThreadPool(2, factory);
    }
}
