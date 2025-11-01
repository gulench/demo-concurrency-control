package com.example.demo.concurrencycontrol.service;

import com.example.demo.concurrencycontrol.model.LineItem;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Service responsible for partitioning lines into groups based on their
 * relationships.
 * Lines with the same groupKey are placed in the same group for serial
 * processing.
 * Lines without a groupKey are placed in individual groups.
 */
@Service
public class LinePartitioningService {

    /**
     * Partitions lines into groups. Related lines (same groupKey) are in the same
     * group.
     * Each group will be processed with internal serial ordering, but groups are
     * processed concurrently.
     *
     * @param lines List of line items to partition
     * @return Map of groupKey to list of lines in that group
     */
    public Map<String, List<LineItem>> partitionLines(List<LineItem> lines) {
        if (lines == null || lines.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, List<LineItem>> groups = new LinkedHashMap<>();
        int independentCounter = 0;

        for (LineItem line : lines) {
            String groupKey = line.getGroupKey();

            // If no groupKey, create a unique group for this line (can be processed
            // independently)
            if (groupKey == null || groupKey.trim().isEmpty()) {
                groupKey = "_independent_" + (independentCounter++);
            }

            groups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(line);
        }

        return groups;
    }

    /**
     * Alternative partitioning strategy: partitions by hash-based distribution for
     * load balancing.
     * Use this when you want to distribute unrelated lines evenly across a fixed
     * number of partitions.
     *
     * @param lines         List of line items
     * @param numPartitions Number of partitions to create
     * @return Map of partition key to list of lines
     */
    public Map<String, List<LineItem>> partitionByHash(List<LineItem> lines, int numPartitions) {
        if (lines == null || lines.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, List<LineItem>> partitions = new LinkedHashMap<>();

        // First, group by explicit groupKey
        Map<String, List<LineItem>> explicitGroups = lines.stream()
                .filter(line -> line.getGroupKey() != null && !line.getGroupKey().trim().isEmpty())
                .collect(Collectors.groupingBy(
                        LineItem::getGroupKey,
                        LinkedHashMap::new,
                        Collectors.toList()
                ));

        // Assign explicit groups to partitions using hash
        for (Map.Entry<String, List<LineItem>> entry : explicitGroups.entrySet()) {
            int partition = Math.abs(entry.getKey().hashCode() % numPartitions);
            String partitionKey = "partition_" + partition;
            partitions.computeIfAbsent(partitionKey, k -> new ArrayList<>()).addAll(entry.getValue());
        }

        // Handle independent lines
        List<LineItem> independentLines = lines.stream()
                .filter(line -> line.getGroupKey() == null || line.getGroupKey().trim().isEmpty())
                .collect(Collectors.toList());

        for (int i = 0; i < independentLines.size(); i++) {
            int partition = i % numPartitions;
            String partitionKey = "partition_" + partition;
            partitions.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(independentLines.get(i));
        }

        return partitions;
    }

    /**
     * Validates that lines within a group maintain ordering based on a sequence
     * field.
     * Useful for ensuring data integrity in related line processing.
     *
     * @param group List of related lines
     * @return true if lines are properly ordered or have no ordering requirement
     */
    public boolean validateGroupOrdering(List<LineItem> group) {
        // Add your custom validation logic here
        // For example, check sequence numbers in metadata
        return true;
    }

    /**
     * Get statistics about the partitioning.
     */
    public PartitionStatistics getStatistics(Map<String, List<LineItem>> groups) {
        int totalGroups = groups.size();
        int totalLines = groups.values().stream().mapToInt(List::size).sum();
        int maxGroupSize = groups.values().stream().mapToInt(List::size).max().orElse(0);
        int minGroupSize = groups.values().stream().mapToInt(List::size).min().orElse(0);
        double avgGroupSize = totalGroups > 0 ? (double) totalLines / totalGroups : 0;

        return new PartitionStatistics(totalGroups, totalLines, minGroupSize, maxGroupSize, avgGroupSize);
    }

    /**
     * Statistics about partitioned groups.
     */
    public static class PartitionStatistics {
        private final int totalGroups;
        private final int totalLines;
        private final int minGroupSize;
        private final int maxGroupSize;
        private final double avgGroupSize;

        public PartitionStatistics(int totalGroups, int totalLines, int minGroupSize,
                int maxGroupSize, double avgGroupSize) {
            this.totalGroups = totalGroups;
            this.totalLines = totalLines;
            this.minGroupSize = minGroupSize;
            this.maxGroupSize = maxGroupSize;
            this.avgGroupSize = avgGroupSize;
        }

        public int getTotalGroups() {
            return totalGroups;
        }

        public int getTotalLines() {
            return totalLines;
        }

        public int getMinGroupSize() {
            return minGroupSize;
        }

        public int getMaxGroupSize() {
            return maxGroupSize;
        }

        public double getAvgGroupSize() {
            return avgGroupSize;
        }

        @Override
        public String toString() {
            return String.format("PartitionStatistics{groups=%d, lines=%d, min=%d, max=%d, avg=%.2f}",
                    totalGroups, totalLines, minGroupSize, maxGroupSize, avgGroupSize);
        }
    }
}