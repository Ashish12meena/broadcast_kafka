package com.aigreentick.services.broadcast.infrastructure.observability;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks in the property that made this class necessary.
 *
 * <p>The original code logged a WARN on every failed Redis call. Because {@code DispatchWorker.run()}
 * calls the rate limiter in a loop with no sleep on the fallback path, a Redis outage produced log
 * volume bounded only by CPU — during the incident when the log pipeline was most needed. These
 * tests fail if anyone reintroduces per-occurrence logging.
 */
class RedisDegradationTrackerTest {

    private static final String LOGGER_NAME = "test.degradation";

    private ch.qos.logback.classic.Logger logger;
    private ListAppender<ILoggingEvent> appender;
    private RedisDegradationTracker tracker;

    @BeforeEach
    void setUp() {
        logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(LOGGER_NAME);
        logger.setLevel(Level.INFO);
        logger.detachAndStopAllAppenders();

        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);

        tracker = new RedisDegradationTracker(logger, "rate limiting");
    }

    @Test
    @DisplayName("a thousand consecutive failures produce exactly one log line")
    void repeatedFailuresLogOnce() {
        for (int i = 0; i < 1_000; i++) {
            tracker.enter("RedisConnectionFailureException: connection refused");
        }

        assertThat(appender.list).hasSize(1);
        assertThat(appender.list.get(0).getLevel()).isEqualTo(Level.WARN);
        assertThat(tracker.isDegraded()).isTrue();
    }

    @Test
    @DisplayName("a full outage cycle produces one WARN and one INFO, whatever the call volume")
    void outageCycleLogsTwice() {
        for (int i = 0; i < 500; i++) {
            tracker.enter("connection refused");
        }
        for (int i = 0; i < 500; i++) {
            tracker.exit();
        }

        List<Level> levels = appender.list.stream().map(ILoggingEvent::getLevel).toList();
        assertThat(levels).containsExactly(Level.WARN, Level.INFO);
        assertThat(tracker.isDegraded()).isFalse();
    }

    @Test
    @DisplayName("exit before any failure logs nothing")
    void exitWhenHealthyIsSilent() {
        for (int i = 0; i < 100; i++) {
            tracker.exit();
        }
        assertThat(appender.list).isEmpty();
    }

    @Test
    @DisplayName("concurrent workers entering the same degradation still log once")
    void concurrentEntryLogsOnce() throws InterruptedException {
        int threads = 32;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int call = 0; call < 100; call++) {
                            tracker.enter("connection refused");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        }

        // One worker per phone number is the real shape; compareAndSet is what makes this exact.
        assertThat(appender.list).hasSize(1);
    }

    @Test
    @DisplayName("a second outage after recovery is reported again")
    void secondOutageIsNotSuppressed() {
        tracker.enter("first");
        tracker.exit();
        tracker.enter("second");

        assertThat(appender.list).hasSize(3);
        assertThat(appender.list.get(2).getLevel()).isEqualTo(Level.WARN);
    }
}
