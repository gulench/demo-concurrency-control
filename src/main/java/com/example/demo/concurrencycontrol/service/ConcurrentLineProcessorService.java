package com.example.demo.concurrencycontrol.service;

import com.example.demo.concurrencycontrol.ConcurrentProcessingConfig;
import com.example.demo.concurrencycontrol.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Core service for processing lines with controlled concurrency at both request
 * and system levels.
 */
@Service
public class ConcurrentLineProcessorService {

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentLineProcessorService.class);

    private final ExecutorService systemExecutor;
    private final ConcurrentProcessingConfig config;
    private final LinePartitioningService partitioningService;
    private final LineProcessingService processingService;

    public ConcurrentLineProcessorService(
            @Qualifier("lineProcessingExecutor") ExecutorService systemExecutor,
            ConcurrentProcessingConfig config,
            LinePartitioningService partitioningService,
            LineProcessingService processingService) {
        this.systemExecutor = systemExecutor;
        this.config = config;
        this.partitioningService = partitioningService;
        this.processingService = processingService;
    }

    /**
     * Process all lines in the request with controlled concurrency.
     * 
     * @param request The request containing lines to process
     * @return Response with results for all lines
     * @throws ProcessingException if processing fails or times out
     */
    public LineProcessingResponse processLines(LineProcessingRequest request) throws ProcessingException {
        long startTime = System.currentTimeMillis();

        logger.info("Starting processing for {} lines", request.getLines().size());

        // Step 1: Partition lines into groups
        Map<String, List<LineItem>> groups = partitioningService.partitionLines(request.getLines());

        var stats = partitioningService.getStatistics(groups);
        logger.info("Partitioned into groups: {}", stats);

        // Step 2: Create a semaphore for request-level concurrency control
        Semaphore requestLevelSemaphore = new Semaphore(config.getMaxConcurrentGroupsPerRequest());

        // Step 3: Process groups concurrently with controlled parallelism
        List<CompletableFuture<List<LineResult>>> groupFutures = new ArrayList<>();

        for (Map.Entry<String, List<LineItem>> entry : groups.entrySet()) {
            String groupKey = entry.getKey();
            List<LineItem> groupLines = entry.getValue();

            CompletableFuture<List<LineResult>> groupFuture = processGroupWithSemaphore(
                    groupKey,
                    groupLines,
                    requestLevelSemaphore
            );

            groupFutures.add(groupFuture);
        }

        // Step 4: Wait for all groups to complete with timeout
        try {
            CompletableFuture<Void> allGroups = CompletableFuture.allOf(
                groupFutures.toArray(new CompletableFuture[0])
            );

            allGroups.get(config.getRequestTimeoutSeconds(), TimeUnit.SECONDS);

        } catch (TimeoutException e) {
            logger.error("Request processing timed out after {} seconds", config.getRequestTimeoutSeconds());
            // Cancel all pending futures
            groupFutures.forEach(f -> f.cancel(true));
            throw new ProcessingException("Request processing timed out", e);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Request processing interrupted", e);
            throw new ProcessingException("Request processing interrupted", e);

        } catch (ExecutionException e) {
            logger.error("Request processing failed", e);
            throw new ProcessingException("Request processing failed", e.getCause());
        }

        // Step 5: Collect all results
        List<LineResult> allResults = groupFutures.stream()
                .map(f -> {
                    try {
                        return f.get(); // Already completed, won't block
                    } catch (Exception e) {
                        logger.error("Failed to retrieve group results", e);
                        return Collections.<LineResult>emptyList();
                    }
                })
                .flatMap(List::stream)
                .collect(Collectors.toList());

        long totalTime = System.currentTimeMillis() - startTime;

        // Step 6: Build response summary
        ProcessingSummary summary = buildSummary(allResults, groups.size(), totalTime);

        logger.info("Completed processing: {}", summary);

        return new LineProcessingResponse(allResults, summary);
    }

    /**
     * Process a single group of lines with semaphore-controlled concurrency.
     */
    private CompletableFuture<List<LineResult>> processGroupWithSemaphore(
            String groupKey,
            List<LineItem> groupLines,
            Semaphore semaphore) {

        return CompletableFuture.supplyAsync(() -> {
            boolean permitAcquired = false;

            try {
                // Acquire permit with timeout (request-level concurrency control)
                permitAcquired = semaphore.tryAcquire(
                        config.getPermitAcquisitionTimeoutSeconds(),
                        TimeUnit.SECONDS
                );

                if (!permitAcquired) {
                    logger.warn("Failed to acquire permit for group {} within timeout", groupKey);
                    return groupLines.stream()
                            .map(line -> LineResult.failure(
                                    line.getLineId(),
                                    groupKey,
                                    "Failed to acquire processing permit"
                            ))
                            .collect(Collectors.toList());
                }

                logger.debug("Processing group {} with {} lines", groupKey, groupLines.size());

                // Process lines in the group serially
                return processGroupSerially(groupKey, groupLines);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while acquiring permit for group {}", groupKey, e);
                return groupLines.stream()
                        .map(line -> LineResult.failure(
                                line.getLineId(),
                                groupKey,
                                "Processing interrupted"
                        ))
                        .collect(Collectors.toList());

            } finally {
                if (permitAcquired) {
                    semaphore.release();
                }
            }
        }, systemExecutor); // Use system-level executor (system concurrency control)
    }

    /**
     * Process lines within a group serially to maintain ordering.
     */
    private List<LineResult> processGroupSerially(String groupKey, List<LineItem> groupLines) {
        List<LineResult> results = new ArrayList<>();

        for (LineItem line : groupLines) {
            long lineStartTime = System.currentTimeMillis();

            try {
                String result = processingService.processLine(line);
                long processingTime = System.currentTimeMillis() - lineStartTime;

                results.add(LineResult.success(
                        line.getLineId(),
                        groupKey,
                        result,
                        processingTime
                ));

                logger.debug("Successfully processed line {} in group {} ({}ms)",
                        line.getLineId(), groupKey, processingTime);

            } catch (Exception e) {
                logger.error("Failed to process line {} in group {}", line.getLineId(), groupKey, e);
                results.add(LineResult.failure(
                        line.getLineId(),
                        groupKey,
                        e.getMessage()
                ));

                // Option: Fail-fast - stop processing rest of group on first error
                // break;

                // Current: Continue processing remaining lines (graceful degradation)
            }
        }

        return results;
    }

    /**
     * Build processing summary from results.
     */
    private ProcessingSummary buildSummary(List<LineResult> results, int totalGroups, long totalTime) {
        int totalLines = results.size();
        int successful = (int) results.stream()
                .filter(r -> r.getStatus() == ProcessingStatus.SUCCESS)
                .count();
        int failed = totalLines - successful;

        return new ProcessingSummary(
                totalLines,
                successful,
                failed,
                totalGroups,
                totalTime
        );
    }

    /**
     * Custom exception for processing failures.
     */
    public static class ProcessingException extends Exception {
        public ProcessingException(String message) {
            super(message);
        }

        public ProcessingException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}