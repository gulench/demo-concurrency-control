package com.example.demo.concurrencycontrol.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Summary of processing execution.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProcessingSummary {

    private int totalLines;
    private int successfulLines;
    private int failedLines;
    private int timeoutLines;
    private int totalGroups;
    private long totalProcessingTimeMs;

    public ProcessingSummary() {
    }

    public ProcessingSummary(int totalLines, int successfulLines, int failedLines,
            int timeoutLines, int totalGroups, long totalProcessingTimeMs) {
        this.totalLines = totalLines;
        this.successfulLines = successfulLines;
        this.failedLines = failedLines;
        this.timeoutLines = timeoutLines;
        this.totalGroups = totalGroups;
        this.totalProcessingTimeMs = totalProcessingTimeMs;
    }

    // Getters and setters
    public int getTotalLines() {
        return totalLines;
    }

    public void setTotalLines(int totalLines) {
        this.totalLines = totalLines;
    }

    public int getSuccessfulLines() {
        return successfulLines;
    }

    public void setSuccessfulLines(int successfulLines) {
        this.successfulLines = successfulLines;
    }

    public int getFailedLines() {
        return failedLines;
    }

    public void setFailedLines(int failedLines) {
        this.failedLines = failedLines;
    }

    public int getTimeoutLines() {
        return timeoutLines;
    }

    public void setTimeoutLines(int timeoutLines) {
        this.timeoutLines = timeoutLines;
    }

    public int getTotalGroups() {
        return totalGroups;
    }

    public void setTotalGroups(int totalGroups) {
        this.totalGroups = totalGroups;
    }

    public long getTotalProcessingTimeMs() {
        return totalProcessingTimeMs;
    }

    public void setTotalProcessingTimeMs(long totalProcessingTimeMs) {
        this.totalProcessingTimeMs = totalProcessingTimeMs;
    }
}
