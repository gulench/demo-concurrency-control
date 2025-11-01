package com.example.demo.concurrencycontrol.service;

import com.example.demo.concurrencycontrol.model.LineItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Service containing the actual business logic for processing individual lines.
 * This is where you implement your domain-specific processing logic.
 */
@Service
public class LineProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(LineProcessingService.class);

    /**
     * Process a single line item. Replace this with your actual business logic.
     * 
     * This method should:
     * - Be thread-safe if it accesses shared resources
     * - Handle its own exceptions appropriately
     * - Return a meaningful result or throw an exception on failure
     * 
     * @param line The line item to process
     * @return Processing result as a string
     * @throws Exception if processing fails
     */
    public String processLine(LineItem line) throws Exception {
        logger.debug("Processing line: {}", line.getLineId());

        // Example processing logic - replace with your actual implementation
        try {
            // Simulate some processing work
            validateLine(line);

            // Simulate I/O or computation
            Thread.sleep(100); // Remove this in production

            // Transform the data
            String processedData = transformData(line);

            // Persist or send to downstream systems
            // persistResult(processedData);

            return processedData;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new Exception("Processing interrupted for line: " + line.getLineId(), e);
        }
    }

    /**
     * Validate line data before processing.
     */
    private void validateLine(LineItem line) throws Exception {
        if (line.getData() == null || line.getData().trim().isEmpty()) {
            throw new Exception("Invalid data for line: " + line.getLineId());
        }

        // Add your validation logic here
        // - Check data format
        // - Verify business rules
        // - Validate against external systems if needed
    }

    /**
     * Transform line data. Replace with your actual transformation logic.
     */
    private String transformData(LineItem line) {
        // Example transformation
        String data = line.getData();
        String metadata = line.getMetadata() != null ? line.getMetadata() : "";

        // Your transformation logic here
        String transformed = String.format("PROCESSED[%s]: %s (metadata: %s)",
                line.getLineId(),
                data.toUpperCase(),
                metadata);

        return transformed;
    }

    /**
     * Example method for processing with external dependencies.
     * In real implementation, this might call databases, REST APIs, message queues,
     * etc.
     */
    public String processWithExternalService(LineItem line) throws Exception {
        // @formatter:off
        // Example: Call external REST API
        // RestTemplate restTemplate = new RestTemplate();
        // ResponseEntity<String> response = restTemplate.postForEntity(
        //     "https://api.example.com/process",
        //     line.getData(),
        //     String.class
        // );
        // return response.getBody();
        // @formatter:on

        return processLine(line);
    }

    /**
     * Example method for database operations.
     * In real implementation, this might use JPA, JDBC, etc.
     */
    public String processWithDatabase(LineItem line) throws Exception {
        // Example: Save to database
        // entityManager.persist(createEntity(line));
        // entityManager.flush();

        return processLine(line);
    }

    /**
     * Batch processing optimization for groups.
     * Use this when you can optimize processing of related lines together.
     */
    public java.util.List<String> processBatch(java.util.List<LineItem> lines) throws Exception {
        // @formatter:off
        // Example: Batch database operations
        // List<Entity> entities = lines.stream()
        //     .map(this::createEntity)
        //     .collect(Collectors.toList());
        // entityManager.persist(entities);
        // @formatter:on

        java.util.List<String> results = new java.util.ArrayList<>();
        for (LineItem line : lines) {
            results.add(processLine(line));
        }
        return results;
    }
}