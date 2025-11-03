package com.example.demo.concurrencycontrol;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.constraints.Min;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for concurrent line processing with system-level and
 * request-level limits.
 * Supports both platform threads and virtual threads (Java 21+).
 */
@Configuration
@ConfigurationProperties(prefix = "app.concurrent.processing")
@Validated
public class ConcurrentProcessingConfig {

    /**
     * Enable virtual threads for processing (requires Java 21+).
     * When true and virtual threads are available, uses virtual thread executor.
     * When false or virtual threads unavailable, uses platform thread pool.
     */
    private boolean useVirtualThreads = true;

    /**
     * Maximum number of threads in the system-wide executor pool.
     * This controls overall system concurrency for PLATFORM threads.
     * Ignored when using virtual threads (virtual threads are unbounded).
     */
    @Min(1)
    private int systemMaxThreads = 20;

    /**
     * Core pool size for the executor (platform threads only).
     */
    @Min(1)
    private int systemCoreThreads = 10;

    /**
     * Queue capacity for pending tasks (platform threads only).
     * Use 0 for direct handoff (SynchronousQueue behavior).
     */
    @Min(0)
    private int queueCapacity = 100;

    /**
     * Maximum concurrent groups allowed per request.
     * This controls request-level concurrency regardless of thread type.
     */
    @Min(1)
    private int maxConcurrentGroupsPerRequest = 5;

    /**
     * Thread keep-alive time in seconds (platform threads only).
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
        if (useVirtualThreads && isVirtualThreadsSupported()) {
            return createVirtualThreadExecutor();
        } else {
            return createPlatformThreadExecutor();
        }
    }

    /**
     * Create virtual thread executor (Java 21+).
     */
    private ExecutorService createVirtualThreadExecutor() {
        try {
            // Use reflection for compatibility with older Java versions
            var executorsClass = Executors.class;
            var method = executorsClass.getMethod("newVirtualThreadPerTaskExecutor");
            ExecutorService executor = (ExecutorService) method.invoke(null);

            System.out.println("✓ Virtual threads enabled for line processing");
            return executor;

        } catch (Exception e) {
            System.err.println(
                    "✗ Failed to create virtual thread executor, falling back to platform threads: " + e.getMessage());
            return createPlatformThreadExecutor();
        }
    }

    /**
     * Create platform thread executor.
     */
    private ExecutorService createPlatformThreadExecutor() {
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

        System.out.println("✓ Platform threads enabled for line processing (max: " + systemMaxThreads + ")");
        return executor;
    }

    /**
     * Check if virtual threads are supported.
     */
    private boolean isVirtualThreadsSupported() {
        try {
            // Check if we're running on Java 21+
            var threadClass = Thread.class;
            threadClass.getMethod("ofVirtual");
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    /**
     * Get the effective concurrency model being used.
     */
    public String getConcurrencyModel() {
        if (useVirtualThreads && isVirtualThreadsSupported()) {
            return "VIRTUAL_THREADS";
        } else {
            return "PLATFORM_THREADS";
        }
    }

    // Getters and setters
    public boolean isUseVirtualThreads() {
        return useVirtualThreads;
    }

    public void setUseVirtualThreads(boolean useVirtualThreads) {
        this.useVirtualThreads = useVirtualThreads;
    }

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
     * Custom thread factory for named platform threads.
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