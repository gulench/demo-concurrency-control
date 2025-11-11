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
 * Supports partial success - lines can succeed or fail independently.
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
     * Supports partial success - returns results for all lines that completed
     * (successfully or with errors),
     * and marks timed-out lines appropriately.
     * 
     * @param request The request containing lines to process
     * @return Response with results for all lines (partial success allowed)
     */
    public LineProcessingResponse processLines(LineProcessingRequest request) {
        long startTime = System.currentTimeMillis();

        logger.info("Starting processing for {} lines", request.getLines().size());

        // Step 1: Partition lines into groups
        Map<String, List<LineItem>> groups = partitioningService.partitionLines(request.getLines());

        var stats = partitioningService.getStatistics(groups);
        logger.info("Partitioned into groups: {}", stats);

        // Step 2: Create a semaphore for request-level concurrency control
        Semaphore requestLevelSemaphore = new Semaphore(config.getMaxConcurrentGroupsPerRequest());

        // Step 3: Track group futures with their metadata
        List<GroupProcessingTask> groupTasks = new ArrayList<>();

        for (Map.Entry<String, List<LineItem>> entry : groups.entrySet()) {
            String groupKey = entry.getKey();
            List<LineItem> groupLines = entry.getValue();

            CompletableFuture<List<LineResult>> groupFuture = processGroupWithSemaphore(
                    groupKey,
                    groupLines,
                    requestLevelSemaphore);

            groupTasks.add(new GroupProcessingTask(groupKey, groupLines, groupFuture));
        }

        // Step 4: Wait for all groups to complete with timeout, collecting partial
        // results
        List<LineResult> allResults = waitForGroupsWithPartialSuccess(
                groupTasks,
                config.getRequestTimeoutSeconds());

        long totalTime = System.currentTimeMillis() - startTime;

        // Step 5: Build response summary
        ProcessingSummary summary = buildSummary(allResults, groups.size(), totalTime);

        logger.info("Completed processing: {} total, {} succeeded, {} failed, {} timeout",
                summary.getTotalLines(),
                summary.getSuccessfulLines(),
                summary.getFailedLines(),
                summary.getTimeoutLines());

        return new LineProcessingResponse(allResults, summary);
    }

    /**
     * Wait for all group futures to complete, handling timeouts gracefully.
     * Returns results for all completed groups and creates timeout results for
     * incomplete groups.
     */
    private List<LineResult> waitForGroupsWithPartialSuccess(
            List<GroupProcessingTask> groupTasks,
            long timeoutSeconds) {

        List<LineResult> allResults = new ArrayList<>();
        long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000);

        // Create a CompletableFuture that completes when all groups complete OR timeout
        // occurs
        CompletableFuture<Void> allGroups = CompletableFuture.allOf(
                groupTasks.stream()
                        .map(task -> task.future)
                        .toArray(CompletableFuture[]::new));

        try {
            // Wait for all groups with timeout
            allGroups.get(timeoutSeconds, TimeUnit.SECONDS);
            logger.info("All {} groups completed successfully within timeout", groupTasks.size());

        } catch (TimeoutException e) {
            logger.warn("Request processing timed out after {} seconds. Collecting partial results.", timeoutSeconds);
            // Don't throw - continue to collect partial results

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Request processing interrupted. Collecting partial results.", e);
            // Don't throw - continue to collect partial results

        } catch (ExecutionException e) {
            logger.warn("Some groups failed during execution. Collecting partial results.", e);
            // Don't throw - continue to collect partial results
        }

        // Collect results from all groups (completed, failed, or timed out)
        for (GroupProcessingTask task : groupTasks) {
            long remainingTime = deadline - System.currentTimeMillis();

            if (task.future.isDone()) {
                // Group completed (successfully or with error)
                try {
                    List<LineResult> groupResults = task.future.getNow(null);
                    if (groupResults != null) {
                        allResults.addAll(groupResults);
                        logger.debug("Collected {} results from completed group {}",
                                groupResults.size(), task.groupKey);
                    } else {
                        // Future completed but returned null - treat as failure
                        allResults.addAll(createFailureResults(
                                task.groupLines,
                                task.groupKey,
                                "Group processing returned null"));
                    }

                } catch (Exception e) {
                    // Exception occurred during group processing
                    logger.error("Group {} failed with exception", task.groupKey, e);
                    allResults.addAll(createFailureResults(
                            task.groupLines,
                            task.groupKey,
                            "Group processing failed: " + e.getMessage()));
                }

            } else if (remainingTime > 100) {
                // Still have time - try to wait a bit more for this specific group
                try {
                    List<LineResult> groupResults = task.future.get(
                            Math.min(remainingTime, 1000),
                            TimeUnit.MILLISECONDS);
                    allResults.addAll(groupResults);
                    logger.debug("Collected {} results from late-completing group {}",
                            groupResults.size(), task.groupKey);

                } catch (TimeoutException e) {
                    // This specific group timed out
                    logger.warn("Group {} timed out", task.groupKey);
                    task.future.cancel(true);
                    allResults.addAll(createTimeoutResults(task.groupLines, task.groupKey));

                } catch (Exception e) {
                    logger.error("Group {} failed", task.groupKey, e);
                    task.future.cancel(true);
                    allResults.addAll(createFailureResults(
                            task.groupLines,
                            task.groupKey,
                            "Group processing failed: " + e.getMessage()));
                }

            } else {
                // No time left - mark as timeout
                logger.warn("Group {} did not complete in time", task.groupKey);
                task.future.cancel(true);
                allResults.addAll(createTimeoutResults(task.groupLines, task.groupKey));
            }
        }

        return allResults;
    }

    /**
     * Process a single group of lines with semaphore-controlled concurrency.
     * Even if permit acquisition fails, returns failure results instead of
     * throwing.
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
                        TimeUnit.SECONDS);

                if (!permitAcquired) {
                    logger.warn("Failed to acquire permit for group {} within timeout", groupKey);
                    return groupLines.stream()
                            .map(line -> LineResult.failure(
                                    line.getLineId(),
                                    groupKey,
                                    "Failed to acquire processing permit (system busy)"))
                            .collect(Collectors.toList());
                }

                logger.debug("Processing group {} with {} lines", groupKey, groupLines.size());

                // Process lines in the group serially (allows partial success within group)
                return processGroupSerially(groupKey, groupLines);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while acquiring permit for group {}", groupKey, e);
                return groupLines.stream()
                        .map(line -> LineResult.failure(
                                line.getLineId(),
                                groupKey,
                                "Processing interrupted"))
                        .collect(Collectors.toList());

            } catch (Exception e) {
                // Catch any unexpected exceptions to ensure we return results
                logger.error("Unexpected error processing group {}", groupKey, e);
                return groupLines.stream()
                        .map(line -> LineResult.failure(
                                line.getLineId(),
                                groupKey,
                                "Unexpected error: " + e.getMessage()))
                        .collect(Collectors.toList());

            } finally {
                if (permitAcquired) {
                    semaphore.release();
                }
            }
        }, systemExecutor).exceptionally(throwable -> {
            // Handle any exceptions from the CompletableFuture itself
            logger.error("CompletableFuture exception for group {}", groupKey, throwable);
            return groupLines.stream()
                    .map(line -> LineResult.failure(
                            line.getLineId(),
                            groupKey,
                            "Async execution failed: " + throwable.getMessage()))
                    .collect(Collectors.toList());
        });
    }

    /**
     * Process lines within a group serially to maintain ordering.
     * Supports partial success - continues processing remaining lines even if one
     * fails.
     */
    private List<LineResult> processGroupSerially(String groupKey, List<LineItem> groupLines) {
        List<LineResult> results = new ArrayList<>();

        for (LineItem line : groupLines) {
            // Check if thread was interrupted (for cancellation support)
            if (Thread.currentThread().isInterrupted()) {
                logger.warn("Thread interrupted while processing group {}. Marking remaining lines as failed.",
                        groupKey);
                // Mark remaining lines as failed due to interruption
                results.add(LineResult.failure(
                        line.getLineId(),
                        groupKey,
                        "Processing interrupted"));
                continue;
            }

            long lineStartTime = System.currentTimeMillis();

            try {
                String result = processingService.processLine(line);
                long processingTime = System.currentTimeMillis() - lineStartTime;

                results.add(LineResult.success(
                        line.getLineId(),
                        groupKey,
                        result,
                        processingTime));

                logger.debug("Successfully processed line {} in group {} ({}ms)",
                        line.getLineId(), groupKey, processingTime);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Processing interrupted for line {} in group {}", line.getLineId(), groupKey);
                results.add(LineResult.failure(
                        line.getLineId(),
                        groupKey,
                        "Processing interrupted"));
                // Continue marking remaining lines as failed

            } catch (Exception e) {
                logger.error("Failed to process line {} in group {}", line.getLineId(), groupKey, e);
                results.add(LineResult.failure(
                        line.getLineId(),
                        groupKey,
                        e.getMessage() != null ? e.getMessage() : "Processing failed"));

                // PARTIAL SUCCESS: Continue processing remaining lines in the group
                // If you want fail-fast behavior for the group, uncomment the line below:
                // break;
            }
        }

        return results;
    }

    /**
     * Create timeout results for lines that didn't complete in time.
     */
    private List<LineResult> createTimeoutResults(List<LineItem> lines, String groupKey) {
        return lines.stream()
                .map(line -> LineResult.timeout(line.getLineId(), groupKey))
                .collect(Collectors.toList());
    }

    /**
     * Create failure results for lines in a failed group.
     */
    private List<LineResult> createFailureResults(List<LineItem> lines, String groupKey, String errorMessage) {
        return lines.stream()
                .map(line -> LineResult.failure(line.getLineId(), groupKey, errorMessage))
                .collect(Collectors.toList());
    }

    /**
     * Build processing summary from results.
     */
    private ProcessingSummary buildSummary(List<LineResult> results, int totalGroups, long totalTime) {
        int totalLines = results.size();
        int successful = (int) results.stream()
                .filter(r -> r.getStatus() == ProcessingStatus.SUCCESS)
                .count();
        int failed = (int) results.stream()
                .filter(r -> r.getStatus() == ProcessingStatus.FAILED)
                .count();
        int timeout = (int) results.stream()
                .filter(r -> r.getStatus() == ProcessingStatus.TIMEOUT)
                .count();

        return new ProcessingSummary(
                totalLines,
                successful,
                failed,
                timeout,
                totalGroups,
                totalTime);
    }

    /**
     * Helper class to track group processing tasks.
     */
    private static class GroupProcessingTask {
        final String groupKey;
        final List<LineItem> groupLines;
        final CompletableFuture<List<LineResult>> future;

        GroupProcessingTask(String groupKey, List<LineItem> groupLines,
                CompletableFuture<List<LineResult>> future) {
            this.groupKey = groupKey;
            this.groupLines = groupLines;
            this.future = future;
        }
    }

    /**
     * Custom exception for processing failures.
     * Note: With partial success support, this is rarely thrown - only for
     * catastrophic failures.
     */
    public static class ProcessingException extends RuntimeException {
        public ProcessingException(String message) {
            super(message);
        }

        public ProcessingException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}