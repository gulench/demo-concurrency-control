package com.example.demo.concurrencycontrol.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Response containing results for all processed lines.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LineProcessingResponse {

    private List<LineResult> results = new ArrayList<>();

    private ProcessingSummary summary;

    public LineProcessingResponse() {
    }

    public LineProcessingResponse(List<LineResult> results, ProcessingSummary summary) {
        this.results = results;
        this.summary = summary;
    }

    public List<LineResult> getResults() {
        return results;
    }

    public void setResults(List<LineResult> results) {
        this.results = results;
    }

    public ProcessingSummary getSummary() {
        return summary;
    }

    public void setSummary(ProcessingSummary summary) {
        this.summary = summary;
    }
}
