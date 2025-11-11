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
 * Supports partial success - lines can succeed or fail independently, even
 * within groups that timeout.
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

        // Step 3: Track group processing tasks with progress tracking
        List<GroupProcessingTask> groupTasks = new ArrayList<>();

        for (Map.Entry<String, List<LineItem>> entry : groups.entrySet()) {
            String groupKey = entry.getKey();
            List<LineItem> groupLines = entry.getValue();

            // Create progress tracker for this group
            GroupProgressTracker progressTracker = new GroupProgressTracker(groupKey, groupLines);

            CompletableFuture<List<LineResult>> groupFuture = processGroupWithSemaphore(
                    groupKey,
                    groupLines,
                    progressTracker,
                    requestLevelSemaphore);

            groupTasks.add(new GroupProcessingTask(groupKey, groupLines, groupFuture, progressTracker));
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
     * incomplete lines.
     * CRITICALLY: Uses progress trackers to get partial results from timed-out
     * groups.
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
                        // Future completed but returned null - use progress tracker
                        allResults.addAll(getPartialResultsFromTracker(task));
                    }

                } catch (Exception e) {
                    // Exception occurred - get whatever was completed from progress tracker
                    logger.error("Group {} failed with exception, retrieving partial results", task.groupKey, e);
                    allResults.addAll(getPartialResultsFromTracker(task));
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
                    // This specific group timed out - get partial results from progress tracker
                    logger.warn("Group {} timed out, retrieving {} partial results",
                            task.groupKey, task.progressTracker.getCompletedCount());
                    task.future.cancel(true);
                    allResults.addAll(getPartialResultsFromTracker(task));

                } catch (Exception e) {
                    logger.error("Group {} failed, retrieving partial results", task.groupKey, e);
                    task.future.cancel(true);
                    allResults.addAll(getPartialResultsFromTracker(task));
                }

            } else {
                // No time left - get partial results from progress tracker
                logger.warn("Group {} did not complete in time, retrieving {} partial results",
                        task.groupKey, task.progressTracker.getCompletedCount());
                task.future.cancel(true);
                allResults.addAll(getPartialResultsFromTracker(task));
            }
        }

        return allResults;
    }

    /**
     * Get partial results from a group's progress tracker.
     * This includes all lines that were successfully processed before
     * timeout/failure,
     * and marks unprocessed lines as TIMEOUT.
     */
    private List<LineResult> getPartialResultsFromTracker(GroupProcessingTask task) {
        List<LineResult> results = new ArrayList<>();

        // Get all completed results (successful or failed)
        results.addAll(task.progressTracker.getCompletedResults());

        // Mark remaining unprocessed lines as timeout
        List<LineItem> unprocessedLines = task.progressTracker.getUnprocessedLines();
        for (LineItem line : unprocessedLines) {
            results.add(LineResult.timeout(line.getLineId(), task.groupKey));
            logger.debug("Marking unprocessed line {} in group {} as TIMEOUT",
                    line.getLineId(), task.groupKey);
        }

        return results;
    }

    /**
     * Process a single group of lines with semaphore-controlled concurrency.
     * Updates progress tracker as lines are processed.
     */
    private CompletableFuture<List<LineResult>> processGroupWithSemaphore(
            String groupKey,
            List<LineItem> groupLines,
            GroupProgressTracker progressTracker,
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
                    // Mark all lines as failed due to permit acquisition failure
                    for (LineItem line : groupLines) {
                        LineResult result = LineResult.failure(
                                line.getLineId(),
                                groupKey,
                                "Failed to acquire processing permit (system busy)");
                        progressTracker.recordResult(line, result);
                    }
                    return progressTracker.getCompletedResults();
                }

                logger.debug("Processing group {} with {} lines", groupKey, groupLines.size());

                // Process lines in the group serially with progress tracking
                return processGroupSerially(groupKey, groupLines, progressTracker);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while acquiring permit for group {}", groupKey, e);
                // Mark unprocessed lines as failed
                for (LineItem line : progressTracker.getUnprocessedLines()) {
                    LineResult result = LineResult.failure(
                            line.getLineId(),
                            groupKey,
                            "Processing interrupted");
                    progressTracker.recordResult(line, result);
                }
                return progressTracker.getCompletedResults();

            } catch (Exception e) {
                // Catch any unexpected exceptions
                logger.error("Unexpected error processing group {}", groupKey, e);
                // Mark unprocessed lines as failed
                for (LineItem line : progressTracker.getUnprocessedLines()) {
                    LineResult result = LineResult.failure(
                            line.getLineId(),
                            groupKey,
                            "Unexpected error: " + e.getMessage());
                    progressTracker.recordResult(line, result);
                }
                return progressTracker.getCompletedResults();

            } finally {
                if (permitAcquired) {
                    semaphore.release();
                }
            }
        }, systemExecutor).exceptionally(throwable -> {
            // Handle any exceptions from the CompletableFuture itself
            logger.error("CompletableFuture exception for group {}", groupKey, throwable);
            // Mark unprocessed lines as failed
            for (LineItem line : progressTracker.getUnprocessedLines()) {
                LineResult result = LineResult.failure(
                        line.getLineId(),
                        groupKey,
                        "Async execution failed: " + throwable.getMessage());
                progressTracker.recordResult(line, result);
            }
            return progressTracker.getCompletedResults();
        });
    }

    /**
     * Process lines within a group serially to maintain ordering.
     * Records progress after each line to support partial success on timeout.
     */
    private List<LineResult> processGroupSerially(String groupKey, List<LineItem> groupLines,
            GroupProgressTracker progressTracker) {
        for (LineItem line : groupLines) {
            // Check if thread was interrupted (for cancellation support)
            if (Thread.currentThread().isInterrupted()) {
                logger.warn("Thread interrupted while processing group {}. Stopping further processing.", groupKey);
                // Remaining lines will be marked as timeout by the caller
                break;
            }

            long lineStartTime = System.currentTimeMillis();

            try {
                String result = processingService.processLine(line);
                long processingTime = System.currentTimeMillis() - lineStartTime;

                LineResult lineResult = LineResult.success(
                        line.getLineId(),
                        groupKey,
                        result,
                        processingTime);

                // CRITICAL: Record result immediately so it's available even if group times out
                progressTracker.recordResult(line, lineResult);

                logger.debug("Successfully processed line {} in group {} ({}ms)",
                        line.getLineId(), groupKey, processingTime);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Processing interrupted for line {} in group {}", line.getLineId(), groupKey);

                LineResult lineResult = LineResult.failure(
                        line.getLineId(),
                        groupKey,
                        "Processing interrupted");
                progressTracker.recordResult(line, lineResult);

                // Stop processing remaining lines
                break;

            } catch (Exception e) {
                logger.error("Failed to process line {} in group {}", line.getLineId(), groupKey, e);

                LineResult lineResult = LineResult.failure(
                        line.getLineId(),
                        groupKey,
                        e.getMessage() != null ? e.getMessage() : "Processing failed");

                // CRITICAL: Record failure immediately
                progressTracker.recordResult(line, lineResult);

                // PARTIAL SUCCESS: Continue processing remaining lines in the group
            }
        }

        return progressTracker.getCompletedResults();
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
        final GroupProgressTracker progressTracker;

        GroupProcessingTask(String groupKey, List<LineItem> groupLines,
                CompletableFuture<List<LineResult>> future,
                GroupProgressTracker progressTracker) {
            this.groupKey = groupKey;
            this.groupLines = groupLines;
            this.future = future;
            this.progressTracker = progressTracker;
        }
    }

    /**
     * Thread-safe progress tracker for a group.
     * Tracks which lines have been processed and their results in real-time.
     */
    private static class GroupProgressTracker {
        private final String groupKey;
        private final List<LineItem> allLines;
        private final Map<String, LineResult> completedResults;
        private final Set<String> processedLineIds;

        public GroupProgressTracker(String groupKey, List<LineItem> allLines) {
            this.groupKey = groupKey;
            this.allLines = new ArrayList<>(allLines);
            this.completedResults = new ConcurrentHashMap<>();
            this.processedLineIds = ConcurrentHashMap.newKeySet();
        }

        /**
         * Record a result for a line (thread-safe).
         */
        public synchronized void recordResult(LineItem line, LineResult result) {
            completedResults.put(line.getLineId(), result);
            processedLineIds.add(line.getLineId());
        }

        /**
         * Get all completed results so far.
         */
        public synchronized List<LineResult> getCompletedResults() {
            return new ArrayList<>(completedResults.values());
        }

        /**
         * Get lines that haven't been processed yet.
         */
        public synchronized List<LineItem> getUnprocessedLines() {
            return allLines.stream()
                    .filter(line -> !processedLineIds.contains(line.getLineId()))
                    .collect(Collectors.toList());
        }

        /**
         * Get count of completed lines.
         */
        public synchronized int getCompletedCount() {
            return completedResults.size();
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