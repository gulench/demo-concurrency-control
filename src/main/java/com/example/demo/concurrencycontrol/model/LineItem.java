package com.example.demo.concurrencycontrol.model;

import jakarta.validation.constraints.NotNull;

public class LineItem {
    @NotNull(message = "Line ID is required")
    private String lineId;

    /**
     * Grouping key - lines with the same groupKey are related and must be processed
     * serially.
     * If null, the line is treated as independent.
     */
    private String groupKey;

    @NotNull(message = "Data is required")
    private String data;

    /**
     * Additional metadata for processing.
     */
    private String metadata;

    public LineItem() {
    }

    public LineItem(String lineId, String groupKey, String data) {
        this.lineId = lineId;
        this.groupKey = groupKey;
        this.data = data;
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

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }
}
