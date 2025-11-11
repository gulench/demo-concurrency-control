package com.example.demo.concurrencycontrol.controller;

import com.example.demo.concurrencycontrol.model.LineProcessingRequest;
import com.example.demo.concurrencycontrol.model.LineProcessingResponse;
import com.example.demo.concurrencycontrol.model.ProcessingSummary;
import com.example.demo.concurrencycontrol.service.ConcurrentLineProcessorService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST Controller for line processing API with concurrent execution.
 */
@RestController
@RequestMapping("/api/v1/lines")
public class LineProcessingController {

    private static final Logger logger = LoggerFactory.getLogger(LineProcessingController.class);

    private final ConcurrentLineProcessorService processorService;
    private final Timer processingTimer;
    private final Counter successCounter;
    private final Counter failureCounter;

    public LineProcessingController(
            ConcurrentLineProcessorService processorService,
            MeterRegistry meterRegistry) {
        this.processorService = processorService;

        // Initialize metrics
        this.processingTimer = Timer.builder("line.processing.duration")
                .description("Time taken to process line requests")
                .register(meterRegistry);

        this.successCounter = Counter.builder("line.processing.success")
                .description("Number of successful line processing requests")
                .register(meterRegistry);

        this.failureCounter = Counter.builder("line.processing.failure")
                .description("Number of failed line processing requests")
                .register(meterRegistry);
    }

    /**
     * Process lines with concurrent execution.
     * 
     * POST /api/v1/lines/process
     * 
     * // @formatter:off
     * Example request:
     * {
     *   "lines": [
     *     {"lineId": "1", "groupKey": "orderA", "data": "item1"},
     *     {"lineId": "2", "groupKey": "orderA", "data": "item2"},
     *     {"lineId": "3", "groupKey": "orderB", "data": "item3"},
     *     {"lineId": "4", "groupKey": null, "data": "item4"}
     *   ]
     * }
     * // @formatter:on
     */
    @PostMapping("/process")
    public ResponseEntity<LineProcessingResponse> processLines(
            @Valid @RequestBody LineProcessingRequest request) {

        logger.info("Received request to process {} lines", request.getLines().size());

        return processingTimer.record(() -> {
            // Process lines - now always returns a response (never throws
            // ProcessingException)
            LineProcessingResponse response = processorService.processLines(request);

            // Determine HTTP status based on results
            boolean hasSuccessful = response.getSummary().getSuccessfulLines() > 0;
            boolean hasFailed = response.getSummary().getFailedLines() > 0
                    || response.getSummary().getTimeoutLines() > 0;

            if (hasSuccessful && !hasFailed) {
                // All lines succeeded
                successCounter.increment();
                logger.info("Successfully processed all {} lines",
                        response.getSummary().getTotalLines());
                return ResponseEntity.ok(response);

            } else if (hasSuccessful && hasFailed) {
                // Partial success - return 207 Multi-Status
                successCounter.increment();
                logger.warn("Partial success: {} succeeded, {} failed, {} timeout",
                        response.getSummary().getSuccessfulLines(),
                        response.getSummary().getFailedLines(),
                        response.getSummary().getTimeoutLines());
                return ResponseEntity.status(HttpStatus.MULTI_STATUS).body(response);

            } else {
                // All lines failed
                failureCounter.increment();
                logger.error("All lines failed: {} failed, {} timeout",
                        response.getSummary().getFailedLines(),
                        response.getSummary().getTimeoutLines());
                return ResponseEntity
                        .status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(response);
            }
        });
    }

    /**
     * Health check endpoint to verify service status.
     * 
     * GET /api/v1/lines/health
     */
    @GetMapping("/health")
    public ResponseEntity<HealthStatus> health() {
        return ResponseEntity.ok(new HealthStatus("UP", "Line processing service is operational"));
    }

    /**
     * Create an error response.
     */
    private LineProcessingResponse createErrorResponse(String errorMessage) {
        LineProcessingResponse response = new LineProcessingResponse();
        ProcessingSummary summary = new ProcessingSummary();
        summary.setTotalLines(0);
        summary.setSuccessfulLines(0);
        summary.setFailedLines(0);
        summary.setTotalGroups(0);
        summary.setTotalProcessingTimeMs(0);
        response.setSummary(summary);
        return response;
    }

    /**
     * Global exception handler for validation errors.
     */
    @ExceptionHandler(org.springframework.web.bind.MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationException(
            org.springframework.web.bind.MethodArgumentNotValidException e) {

        String errorMessage = e.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .reduce((a, b) -> a + ", " + b)
                .orElse("Validation failed");

        logger.warn("Validation error: {}", errorMessage);

        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(new ErrorResponse("VALIDATION_ERROR", errorMessage));
    }

    /**
     * Global exception handler for unexpected errors.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpectedException(Exception e) {
        logger.error("Unexpected error", e);

        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse("INTERNAL_ERROR", "An unexpected error occurred"));
    }

    /**
     * Health status response.
     */
    public static class HealthStatus {
        private String status;
        private String message;

        public HealthStatus(String status, String message) {
            this.status = status;
            this.message = message;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }

    /**
     * Error response.
     */
    public static class ErrorResponse {
        private String errorCode;
        private String errorMessage;

        public ErrorResponse(String errorCode, String errorMessage) {
            this.errorCode = errorCode;
            this.errorMessage = errorMessage;
        }

        public String getErrorCode() {
            return errorCode;
        }

        public void setErrorCode(String errorCode) {
            this.errorCode = errorCode;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }
    }
}