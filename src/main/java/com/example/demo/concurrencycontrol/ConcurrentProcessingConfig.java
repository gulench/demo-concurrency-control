package com.example.demo.concurrencycontrol;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.constraints.Min;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for concurrent line processing with system-level and
 * request-level limits.
 */
@Configuration
@ConfigurationProperties(prefix = "app.concurrent.processing")
@Validated
public class ConcurrentProcessingConfig {

    /**
     * Maximum number of threads in the system-wide executor pool.
     * This controls overall system concurrency.
     */
    @Min(1)
    private int systemMaxThreads = 20;

    /**
     * Core pool size for the executor.
     */
    @Min(1)
    private int systemCoreThreads = 10;

    /**
     * Queue capacity for pending tasks.
     * Use 0 for direct handoff (SynchronousQueue behavior).
     */
    @Min(0)
    private int queueCapacity = 100;

    /**
     * Maximum concurrent groups allowed per request.
     * This controls request-level concurrency.
     */
    @Min(1)
    private int maxConcurrentGroupsPerRequest = 5;

    /**
     * Thread keep-alive time in seconds.
     */
    @Min(1)
    private long threadKeepAliveSeconds = 60;

    /**
     * Timeout for acquiring request-level semaphore permit (in seconds).
     */
    @Min(1)
    private long permitAcquisitionTimeoutSeconds = 30;

    /**
     * Overall timeout for request processing (in seconds).
     */
    @Min(1)
    private long requestTimeoutSeconds = 300;

    @Bean(name = "lineProcessingExecutor", destroyMethod = "shutdown")
    public ExecutorService lineProcessingExecutor() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                systemCoreThreads,
                systemMaxThreads,
                threadKeepAliveSeconds,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                new NamedThreadFactory("line-processor"),
                new ThreadPoolExecutor.CallerRunsPolicy() // Fallback policy
        );

        // Allow core threads to timeout
        executor.allowCoreThreadTimeOut(true);

        return executor;
    }

    // Getters and setters
    public int getSystemMaxThreads() {
        return systemMaxThreads;
    }

    public void setSystemMaxThreads(int systemMaxThreads) {
        this.systemMaxThreads = systemMaxThreads;
    }

    public int getSystemCoreThreads() {
        return systemCoreThreads;
    }

    public void setSystemCoreThreads(int systemCoreThreads) {
        this.systemCoreThreads = systemCoreThreads;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public int getMaxConcurrentGroupsPerRequest() {
        return maxConcurrentGroupsPerRequest;
    }

    public void setMaxConcurrentGroupsPerRequest(int maxConcurrentGroupsPerRequest) {
        this.maxConcurrentGroupsPerRequest = maxConcurrentGroupsPerRequest;
    }

    public long getThreadKeepAliveSeconds() {
        return threadKeepAliveSeconds;
    }

    public void setThreadKeepAliveSeconds(long threadKeepAliveSeconds) {
        this.threadKeepAliveSeconds = threadKeepAliveSeconds;
    }

    public long getPermitAcquisitionTimeoutSeconds() {
        return permitAcquisitionTimeoutSeconds;
    }

    public void setPermitAcquisitionTimeoutSeconds(long permitAcquisitionTimeoutSeconds) {
        this.permitAcquisitionTimeoutSeconds = permitAcquisitionTimeoutSeconds;
    }

    public long getRequestTimeoutSeconds() {
        return requestTimeoutSeconds;
    }

    public void setRequestTimeoutSeconds(long requestTimeoutSeconds) {
        this.requestTimeoutSeconds = requestTimeoutSeconds;
    }

    /**
     * Custom thread factory for named threads.
     */
    private static class NamedThreadFactory implements java.util.concurrent.ThreadFactory {
        private final String prefix;
        private final java.util.concurrent.atomic.AtomicInteger counter = new java.util.concurrent.atomic.AtomicInteger(
                0);

        public NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, prefix + "-" + counter.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        }
    }
}