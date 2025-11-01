package com.example.demo.concurrencycontrol.model;

public class LineResult {
    private String lineId;
    private String groupKey;
    private ProcessingStatus status;
    private String result;
    private String errorMessage;
    private Long processingTimeMs;

    public LineResult() {
    }

    public LineResult(String lineId, String groupKey, ProcessingStatus status) {
        this.lineId = lineId;
        this.groupKey = groupKey;
        this.status = status;
    }

    public static LineResult success(String lineId, String groupKey, String result, long processingTimeMs) {
        LineResult lr = new LineResult(lineId, groupKey, ProcessingStatus.SUCCESS);
        lr.setResult(result);
        lr.setProcessingTimeMs(processingTimeMs);
        return lr;
    }

    public static LineResult failure(String lineId, String groupKey, String errorMessage) {
        LineResult lr = new LineResult(lineId, groupKey, ProcessingStatus.FAILED);
        lr.setErrorMessage(errorMessage);
        return lr;
    }

    // Getters and setters
    public String getLineId() {
        return lineId;
    }

    public void setLineId(String lineId) {
        this.lineId = lineId;
    }

    public String getGroupKey() {
        return groupKey;
    }

    public void setGroupKey(String groupKey) {
        this.groupKey = groupKey;
    }

    public ProcessingStatus getStatus() {
        return status;
    }

    public void setStatus(ProcessingStatus status) {
        this.status = status;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Long getProcessingTimeMs() {
        return processingTimeMs;
    }

    public void setProcessingTimeMs(Long processingTimeMs) {
        this.processingTimeMs = processingTimeMs;
    }
}
