package com.example.demo.concurrencycontrol.service;

import com.example.demo.concurrencycontrol.ConcurrentProcessingConfig;
import com.example.demo.concurrencycontrol.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for concurrent line processing.
 */
@ExtendWith(MockitoExtension.class)
class ConcurrentLineProcessorServiceTest {

    @Mock
    private LineProcessingService processingService;

    private ConcurrentLineProcessorService processorService;
    private ExecutorService executor;
    private ConcurrentProcessingConfig config;
    private LinePartitioningService partitioningService;

    @BeforeEach
    void setUp() {
        // Create real executor for testing
        executor = Executors.newFixedThreadPool(4);

        // Create config
        config = new ConcurrentProcessingConfig();
        config.setSystemMaxThreads(4);
        config.setSystemCoreThreads(2);
        config.setMaxConcurrentGroupsPerRequest(2);
        config.setPermitAcquisitionTimeoutSeconds(5);
        config.setRequestTimeoutSeconds(10);

        // Create real partitioning service
        partitioningService = new LinePartitioningService();

        // Create service under test
        processorService = new ConcurrentLineProcessorService(
                executor,
                config,
                partitioningService,
                processingService);
    }

    @Test
    void testProcessLines_Success() throws Exception {
        // Arrange
        when(processingService.processLine(any()))
                .thenReturn("PROCESSED");

        LineProcessingRequest request = createTestRequest(5, 2);

        // Act
        LineProcessingResponse response = processorService.processLines(request);

        // Assert
        assertNotNull(response);
        assertEquals(5, response.getSummary().getTotalLines());
        assertEquals(5, response.getSummary().getSuccessfulLines());
        assertEquals(0, response.getSummary().getFailedLines());
        assertEquals(5, response.getResults().size());

        // Verify all lines were processed
        verify(processingService, times(5)).processLine(any());
    }

    @Test
    void testProcessLines_WithGrouping() throws Exception {
        // Arrange
        when(processingService.processLine(any()))
                .thenReturn("PROCESSED");

        LineProcessingRequest request = new LineProcessingRequest();
        request.setLines(Arrays.asList(
                new LineItem("1", "groupA", "data1"),
                new LineItem("2", "groupA", "data2"),
                new LineItem("3", "groupB", "data3"),
                new LineItem("4", null, "data4")));

        // Act
        LineProcessingResponse response = processorService.processLines(request);

        // Assert
        assertEquals(4, response.getSummary().getTotalLines());
        assertEquals(4, response.getSummary().getSuccessfulLines());
        assertTrue(response.getSummary().getTotalGroups() >= 3); // groupA, groupB, independent
    }

    @Test
    void testProcessLines_PartialFailure() throws Exception {
        // Arrange
        when(processingService.processLine(any()))
                .thenReturn("PROCESSED")
                .thenThrow(new RuntimeException("Processing failed"))
                .thenReturn("PROCESSED");

        LineProcessingRequest request = createTestRequest(3, 1);

        // Act
        LineProcessingResponse response = processorService.processLines(request);

        // Assert
        assertEquals(3, response.getSummary().getTotalLines());
        assertEquals(2, response.getSummary().getSuccessfulLines());
        assertEquals(1, response.getSummary().getFailedLines());

        // Verify failed line has error message
        long failedCount = response.getResults().stream()
                .filter(r -> r.getStatus() == ProcessingStatus.FAILED)
                .count();
        assertEquals(1, failedCount);
    }

    @Test
    void testProcessLines_Timeout() {
        // Arrange
        try {
            when(processingService.processLine(any())).thenAnswer(invocation -> {
                Thread.sleep(15000); // Longer than request timeout
                return "PROCESSED";
            });
        } catch (Exception e) {
            fail("Setup failed");
        }

        LineProcessingRequest request = createTestRequest(3, 1);

        // Act & Assert

        // Will no longer throw Exception on timeout
        // assertThrows(ConcurrentLineProcessorService.ProcessingException.class, () ->
        // {
        // processorService.processLines(request);
        // });
        var response = processorService.processLines(request);

        // Assert
        assertEquals(3, response.getSummary().getTotalLines());
        assertEquals(0, response.getSummary().getSuccessfulLines());
        assertEquals(1, response.getSummary().getFailedLines());
        assertEquals(2, response.getSummary().getTimeoutLines());

        // Verify failed line have FAILED status
        long failedCount = response.getResults().stream()
                .filter(r -> r.getStatus() == ProcessingStatus.FAILED)
                .count();
        assertEquals(1, failedCount);

        // Verify timed out line have TIMEOUT status
        long timedOutCount = response.getResults().stream()
                .filter(r -> r.getStatus() == ProcessingStatus.TIMEOUT)
                .count();
        assertEquals(2, timedOutCount);
    }

    @Test
    void testConcurrencyControl_RequestLevel() throws Exception {
        // Arrange
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch concurrencyLatch = new CountDownLatch(config.getMaxConcurrentGroupsPerRequest());

        when(processingService.processLine(any())).thenAnswer(invocation -> {
            startLatch.await(); // Wait for signal
            concurrencyLatch.countDown();
            Thread.sleep(100);
            return "PROCESSED";
        });

        // Create request with more groups than allowed concurrency
        LineProcessingRequest request = createTestRequest(10, 1);

        // Act
        CompletableFuture<LineProcessingResponse> future = CompletableFuture.supplyAsync(() -> {
            try {
                return processorService.processLines(request);
            } catch (ConcurrentLineProcessorService.ProcessingException e) {
                fail("Processing failed: " + e.getMessage());
                return null;
            }
        });

        // Release processing
        startLatch.countDown();

        // Wait for max concurrent groups to start
        boolean reachedConcurrency = concurrencyLatch.await(2, TimeUnit.SECONDS);

        // Assert
        assertTrue(reachedConcurrency, "Should reach configured concurrency level");

        // Wait for completion
        LineProcessingResponse response = future.get(15, TimeUnit.SECONDS);
        assertNotNull(response);
    }

    @Test
    void testPartitioning_RelatedLines() {
        // Arrange
        LineProcessingRequest request = new LineProcessingRequest();
        request.setLines(Arrays.asList(
                new LineItem("1", "orderA", "data1"),
                new LineItem("2", "orderA", "data2"),
                new LineItem("3", "orderB", "data3"),
                new LineItem("4", "orderB", "data4"),
                new LineItem("5", null, "data5")));

        // Act
        Map<String, List<LineItem>> groups = partitioningService.partitionLines(request.getLines());

        // Assert
        assertEquals(2, groups.get("orderA").size());
        assertEquals(2, groups.get("orderB").size());
        assertEquals(3, groups.size()); // orderA, orderB, and one independent
    }

    @Test
    void testPartitioning_Statistics() {
        // Arrange
        LineProcessingRequest request = createTestRequest(10, 3);
        Map<String, List<LineItem>> groups = partitioningService.partitionLines(request.getLines());

        // Act
        var stats = partitioningService.getStatistics(groups);

        // Assert
        assertEquals(10, stats.getTotalLines());
        assertTrue(stats.getTotalGroups() > 0);
        assertTrue(stats.getAvgGroupSize() > 0);
    }

    // Helper methods

    private LineProcessingRequest createTestRequest(int numLines, int linesPerGroup) {
        LineProcessingRequest request = new LineProcessingRequest();
        List<LineItem> lines = new ArrayList<>();

        int groupCounter = 0;
        for (int i = 0; i < numLines; i++) {
            if (i % linesPerGroup == 0) {
                groupCounter++;
            }
            String groupKey = linesPerGroup > 0 ? "group" + groupCounter : null;
            lines.add(new LineItem("line" + i, groupKey, "data" + i));
        }

        request.setLines(lines);
        return request;
    }
}

/**
 * Integration tests for the complete processing pipeline.
 */
@ExtendWith(MockitoExtension.class)
class LinePartitioningServiceTest {

    private LinePartitioningService service;

    @BeforeEach
    void setUp() {
        service = new LinePartitioningService();
    }

    @Test
    void testPartitionByHash() {
        // Arrange
        List<LineItem> lines = Arrays.asList(
                new LineItem("1", "A", "data1"),
                new LineItem("2", "A", "data2"),
                new LineItem("3", "B", "data3"),
                new LineItem("4", null, "data4"),
                new LineItem("5", null, "data5"));

        // Act
        Map<String, List<LineItem>> partitions = service.partitionByHash(lines, 3);

        // Assert
        assertFalse(partitions.isEmpty());

        // Verify all lines are in some partition
        int totalLines = partitions.values().stream()
                .mapToInt(List::size)
                .sum();
        assertEquals(5, totalLines);

        // Verify related lines (same groupKey) are in the same partition
        Map<String, Set<String>> groupToPartitions = new HashMap<>();
        for (Map.Entry<String, List<LineItem>> entry : partitions.entrySet()) {
            for (LineItem line : entry.getValue()) {
                if (line.getGroupKey() != null) {
                    groupToPartitions.computeIfAbsent(line.getGroupKey(), k -> new HashSet<>())
                            .add(entry.getKey());
                }
            }
        }

        // Lines with groupKey "A" should all be in the same partition
        assertEquals(1, groupToPartitions.get("A").size());
        assertEquals(1, groupToPartitions.get("B").size());
    }
}
