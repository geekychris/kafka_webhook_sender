package com.example.kafkawebhooksender.controller;

import com.example.kafkawebhooksender.model.StoredEvent;
import com.example.kafkawebhooksender.repository.EventRepository;
import com.example.kafkawebhooksender.service.EventDeliveryService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.Valid;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Controller for injecting events into the system for testing purposes.
 * Supports both direct injection (bypassing Kafka) and Kafka-based injection.
 */
@RestController
@RequestMapping("/api/testing")
public class EventInjectionController {
    
    private static final Logger logger = LoggerFactory.getLogger(EventInjectionController.class);
    
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final EventRepository eventRepository;
    private final EventDeliveryService eventDeliveryService;
    private final ObjectMapper objectMapper;
    
    private final Counter directInjectionCounter;
    private final Counter kafkaInjectionCounter;
    
    public EventInjectionController(
            KafkaTemplate<String, String> kafkaTemplate,
            EventRepository eventRepository,
            EventDeliveryService eventDeliveryService,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {
        
        this.kafkaTemplate = kafkaTemplate;
        this.eventRepository = eventRepository;
        this.eventDeliveryService = eventDeliveryService;
        this.objectMapper = objectMapper;
        
        this.directInjectionCounter = Counter.builder("testing.event.direct_injection")
                .description("Number of events injected directly into the processing pipeline")
                .register(meterRegistry);
        
        this.kafkaInjectionCounter = Counter.builder("testing.event.kafka_injection")
                .description("Number of events injected via Kafka")
                .register(meterRegistry);
    }
    
    /**
     * Inject an event directly into the processing pipeline (bypassing Kafka)
     */
    @PostMapping("/inject-direct")
    public ResponseEntity<EventInjectionResponse> injectEventDirect(@Valid @RequestBody EventInjectionRequest request) {
        try {
            // Create CloudEvent from the request
            CloudEvent cloudEvent = createCloudEvent(request);
            
            // Convert CloudEvent to JSON string for storage
            String eventJson = objectMapper.writeValueAsString(convertCloudEventToMap(cloudEvent));
            
            // Store the event directly in the event repository
            StoredEvent storedEvent = eventRepository.store(request.to(), cloudEvent);
            
            // Increment metrics
            directInjectionCounter.increment();
            
            logger.info("Directly injected event [{}] for recipient [{}]", cloudEvent.getId(), request.to());
            
            return ResponseEntity.ok(new EventInjectionResponse(
                    "success",
                    "Event injected directly into processing pipeline",
                    cloudEvent.getId(),
                    request.to(),
                    "direct",
                    Instant.now().toString()
            ));
            
        } catch (Exception e) {
            logger.error("Failed to inject event directly: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(new EventInjectionResponse(
                    "error",
                    "Failed to inject event: " + e.getMessage(),
                    null,
                    request.to(),
                    "direct",
                    Instant.now().toString()
            ));
        }
    }
    
    /**
     * Inject an event via Kafka (normal flow)
     */
    @PostMapping("/inject-kafka")
    public ResponseEntity<EventInjectionResponse> injectEventViaKafka(@Valid @RequestBody EventInjectionRequest request) {
        try {
            // Create CloudEvent from the request
            CloudEvent cloudEvent = createCloudEvent(request);
            
            // Convert to JSON and send to Kafka
            String eventJson = objectMapper.writeValueAsString(convertCloudEventToMap(cloudEvent));
            
            kafkaTemplate.send("events", request.to(), eventJson);
            
            // Increment metrics
            kafkaInjectionCounter.increment();
            
            logger.info("Injected event [{}] via Kafka for recipient [{}]", cloudEvent.getId(), request.to());
            
            return ResponseEntity.ok(new EventInjectionResponse(
                    "success",
                    "Event injected via Kafka",
                    cloudEvent.getId(),
                    request.to(),
                    "kafka",
                    Instant.now().toString()
            ));
            
        } catch (Exception e) {
            logger.error("Failed to inject event via Kafka: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(new EventInjectionResponse(
                    "error",
                    "Failed to inject event: " + e.getMessage(),
                    null,
                    request.to(),
                    "kafka",
                    Instant.now().toString()
            ));
        }
    }
    
    /**
     * Trigger delivery of pending events for a specific recipient
     */
    @PostMapping("/trigger-delivery/{to}")
    public ResponseEntity<DeliveryTriggerResponse> triggerDelivery(@PathVariable String to) {
        try {
            eventDeliveryService.triggerDelivery(to);
            logger.info("Triggered delivery for recipient [{}]", to);
            
            return ResponseEntity.ok(new DeliveryTriggerResponse(
                    "success",
                    "Delivery triggered for recipient: " + to,
                    to,
                    Instant.now().toString()
            ));
            
        } catch (Exception e) {
            logger.error("Failed to trigger delivery for recipient [{}]: {}", to, e.getMessage(), e);
            return ResponseEntity.internalServerError().body(new DeliveryTriggerResponse(
                    "error",
                    "Failed to trigger delivery: " + e.getMessage(),
                    to,
                    Instant.now().toString()
            ));
        }
    }
    
    /**
     * Get testing status and statistics
     */
    @GetMapping("/status")
    public ResponseEntity<TestingStatus> getTestingStatus() {
        try {
            // Note: The current EventRepository doesn't have a findAll method,
            // so we'll provide basic metrics from the counters
            // In a production system, you'd want to add methods to get total event counts
            
            return ResponseEntity.ok(new TestingStatus(
                    true,
                    (long) (directInjectionCounter.count() + kafkaInjectionCounter.count()),
                    0, // We don't track processed status in the current model
                    0, // We don't track pending status in the current model
                    (double) directInjectionCounter.count(),
                    (double) kafkaInjectionCounter.count()
            ));
            
        } catch (Exception e) {
            logger.error("Failed to get testing status: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(new TestingStatus(
                    false, 0, 0, 0, 0, 0
            ));
        }
    }
    
    /**
     * Create a simple test event for quick testing
     */
    @PostMapping("/create-test-event/{to}")
    public ResponseEntity<EventInjectionResponse> createTestEvent(
            @PathVariable String to,
            @RequestParam(defaultValue = "direct") String method) {
        
        EventInjectionRequest request = new EventInjectionRequest(
                to,
                "user.test",
                "Test Event " + Instant.now(),
                Map.of(
                        "userId", to,
                        "testData", "This is a test event created at " + Instant.now(),
                        "environment", "testing"
                )
        );
        
        if ("kafka".equals(method)) {
            return injectEventViaKafka(request);
        } else {
            return injectEventDirect(request);
        }
    }
    
    // Helper methods
    private CloudEvent createCloudEvent(EventInjectionRequest request) throws JsonProcessingException {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withType(request.eventType())
                .withSource(URI.create("urn:example:testing"))
                .withSubject(request.to())
                .withTime(OffsetDateTime.now(ZoneOffset.UTC))
                .withData("application/json", objectMapper.writeValueAsBytes(request.data()))
                .build();
    }
    
    private Map<String, Object> convertCloudEventToMap(CloudEvent cloudEvent) throws JsonProcessingException, java.io.IOException {
        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("specversion", cloudEvent.getSpecVersion().toString());
        eventMap.put("id", cloudEvent.getId());
        eventMap.put("type", cloudEvent.getType());
        eventMap.put("source", cloudEvent.getSource().toString());
        eventMap.put("subject", cloudEvent.getSubject());
        eventMap.put("time", cloudEvent.getTime().toString());
        
        if (cloudEvent.getData() != null) {
            eventMap.put("data", objectMapper.readValue(cloudEvent.getData().toBytes(), JsonNode.class));
        }
        
        return eventMap;
    }
    
    // DTOs
    public record EventInjectionRequest(
            @jakarta.validation.constraints.NotBlank String to,
            @jakarta.validation.constraints.NotBlank String eventType,
            String description,
            @jakarta.validation.constraints.NotNull Map<String, Object> data
    ) {}
    
    public record EventInjectionResponse(
            String status,
            String message,
            String eventId,
            String to,
            String method,
            String timestamp
    ) {}
    
    public record DeliveryTriggerResponse(
            String status,
            String message,
            String to,
            String timestamp
    ) {}
    
    public record TestingStatus(
            boolean active,
            long totalEvents,
            long processedEvents,
            long pendingEvents,
            double directInjectionsCount,
            double kafkaInjectionsCount
    ) {}
}