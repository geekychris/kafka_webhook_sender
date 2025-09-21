package com.example.kafkawebhooksender.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Loopback webhook controller for testing webhook delivery.
 * This controller acts as a webhook endpoint that can receive events from the webhook sender itself.
 */
@RestController
@RequestMapping("/api/loopback")
public class LoopbackWebhookController {
    
    private static final Logger logger = LoggerFactory.getLogger(LoopbackWebhookController.class);
    
    private final ObjectMapper objectMapper;
    private final Counter webhookReceiveCounter;
    private final AtomicLong eventCounter = new AtomicLong(0);
    
    // Store received events for inspection (in production, this would be a proper storage)
    private final Map<String, ReceivedWebhookEvent> receivedEvents = new ConcurrentHashMap<>();
    
    public LoopbackWebhookController(ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        this.objectMapper = objectMapper;
        this.webhookReceiveCounter = Counter.builder("loopback.webhook.received")
                .description("Number of webhook events received by loopback endpoint")
                .register(meterRegistry);
    }
    
    /**
     * Main webhook endpoint that receives events
     */
    @PostMapping("/webhook")
    public ResponseEntity<WebhookResponse> receiveWebhook(
            @RequestBody String payload,
            @RequestHeader Map<String, String> headers) {
        
        long eventId = eventCounter.incrementAndGet();
        String eventKey = "event-" + eventId;
        
        try {
            // Parse the payload as JSON
            JsonNode eventData = objectMapper.readTree(payload);
            
            // Create received event record
            ReceivedWebhookEvent receivedEvent = new ReceivedWebhookEvent(
                    eventKey,
                    payload,
                    headers,
                    Instant.now()
            );
            
            // Store the event
            receivedEvents.put(eventKey, receivedEvent);
            
            // Log the received event
            logger.info("Received webhook event [{}]: payload={}, headers={}", 
                    eventKey, payload, headers);
            
            // Increment metrics
            webhookReceiveCounter.increment();
            
            // Return success response
            return ResponseEntity.ok(new WebhookResponse(
                    "success", 
                    "Event received successfully", 
                    eventKey,
                    Instant.now().toString()
            ));
            
        } catch (Exception e) {
            logger.error("Failed to process webhook event [{}]: {}", eventKey, e.getMessage(), e);
            
            // Still store the event for debugging
            ReceivedWebhookEvent receivedEvent = new ReceivedWebhookEvent(
                    eventKey,
                    payload,
                    headers,
                    Instant.now()
            );
            receivedEvents.put(eventKey, receivedEvent);
            
            return ResponseEntity.badRequest().body(new WebhookResponse(
                    "error", 
                    "Failed to process event: " + e.getMessage(), 
                    eventKey,
                    Instant.now().toString()
            ));
        }
    }
    
    /**
     * Get all received events
     */
    @GetMapping("/events")
    public ResponseEntity<Map<String, ReceivedWebhookEvent>> getAllReceivedEvents() {
        return ResponseEntity.ok(receivedEvents);
    }
    
    /**
     * Get a specific received event by ID
     */
    @GetMapping("/events/{eventId}")
    public ResponseEntity<ReceivedWebhookEvent> getReceivedEvent(@PathVariable String eventId) {
        ReceivedWebhookEvent event = receivedEvents.get(eventId);
        if (event == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(event);
    }
    
    /**
     * Clear all received events (useful for testing)
     */
    @DeleteMapping("/events")
    public ResponseEntity<ClearResponse> clearReceivedEvents() {
        int count = receivedEvents.size();
        receivedEvents.clear();
        logger.info("Cleared {} received webhook events", count);
        return ResponseEntity.ok(new ClearResponse("success", "Cleared " + count + " events"));
    }
    
    /**
     * Get loopback status and statistics
     */
    @GetMapping("/status")
    public ResponseEntity<LoopbackStatus> getStatus() {
        return ResponseEntity.ok(new LoopbackStatus(
                true,
                receivedEvents.size(),
                eventCounter.get(),
                receivedEvents.isEmpty() ? null : 
                    receivedEvents.values().stream()
                        .map(e -> e.receivedAt())
                        .max(Instant::compareTo)
                        .map(instant -> instant.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT))
                        .orElse(null)
        ));
    }
    
    // DTOs for responses
    public record WebhookResponse(
            String status,
            String message,
            String eventId,
            String timestamp
    ) {}
    
    public record ReceivedWebhookEvent(
            String eventId,
            String payload,
            Map<String, String> headers,
            Instant receivedAt
    ) {}
    
    public record ClearResponse(
            String status,
            String message
    ) {}
    
    public record LoopbackStatus(
            boolean active,
            int totalEventsReceived,
            long eventCounter,
            String lastEventReceived
    ) {}
}