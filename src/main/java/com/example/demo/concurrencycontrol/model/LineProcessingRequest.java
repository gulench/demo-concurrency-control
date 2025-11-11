package com.example.demo.concurrencycontrol.model;

import java.util.ArrayList;
import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

/**
 * Request containing multiple lines to be processed.
 */
public class LineProcessingRequest {

    @NotEmpty(message = "Lines cannot be empty")
    @Valid
    private List<LineItem> lines = new ArrayList<>();

    public LineProcessingRequest() {
    }

    public LineProcessingRequest(List<LineItem> lines) {
        this.lines = lines;
    }

    public List<LineItem> getLines() {
        return lines;
    }

    public void setLines(List<LineItem> lines) {
        this.lines = lines;
    }
}
