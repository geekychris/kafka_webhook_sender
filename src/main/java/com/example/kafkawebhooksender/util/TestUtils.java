package com.example.kafkawebhooksender.util;

import com.example.kafkawebhooksender.controller.EventInjectionController;
import com.example.kafkawebhooksender.model.DeliveryMethod;
import com.example.kafkawebhooksender.model.Subscription;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Utility class for testing functionality
 */
@Component
public class TestUtils {
    
    private final ObjectMapper objectMapper;
    
    public TestUtils(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }
    
    /**
     * Create a simple loopback subscription for testing
     */
    public Subscription.Builder createLoopbackSubscriptionBuilder(String to, String baseUrl) {
        return Subscription.builder()
                .to(to)
                .deliveryMethod(DeliveryMethod.WEBHOOK)
                .endpoint(baseUrl + "/api/loopback/webhook")
                .headers(Map.of(
                        "X-Test-Header", "test-" + to,
                        "Authorization", "Bearer test-token-" + UUID.randomUUID().toString().substring(0, 8)
                ))
                .configuration(Map.of())
                .enabled(true)
                .maxRetries(3)
                .retryDelayMs(1000L)
                .batchSize(1);
    }
    
    /**
     * Create a test event injection request
     */
    public EventInjectionController.EventInjectionRequest createTestEventRequest(String to, String eventType) {
        return createTestEventRequest(to, eventType, Map.of(
                "testId", to + "-" + System.currentTimeMillis(),
                "timestamp", Instant.now().toString(),
                "testData", "Generated test event for " + to
        ));
    }
    
    /**
     * Create a test event injection request with custom data
     */
    public EventInjectionController.EventInjectionRequest createTestEventRequest(String to, String eventType, Map<String, Object> data) {
        return new EventInjectionController.EventInjectionRequest(
                to,
                eventType,
                "Test event: " + eventType + " for " + to,
                data
        );
    }
    
    /**
     * Create a comprehensive test event with nested data structures
     */
    public EventInjectionController.EventInjectionRequest createComplexTestEvent(String to) {
        Map<String, Object> data = new HashMap<>();
        data.put("userId", to);
        data.put("eventId", UUID.randomUUID().toString());
        data.put("timestamp", Instant.now().toString());
        data.put("version", "1.0");
        
        // Add nested objects
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("source", "test-suite");
        metadata.put("environment", "integration");
        metadata.put("testCase", "complex-event");
        data.put("metadata", metadata);
        
        // Add arrays
        data.put("tags", new String[]{"test", "integration", "complex"});
        
        // Add different data types
        Map<String, Object> payload = new HashMap<>();
        payload.put("stringValue", "test-string");
        payload.put("intValue", 42);
        payload.put("doubleValue", 3.14159);
        payload.put("booleanValue", true);
        data.put("payload", payload);
        
        return new EventInjectionController.EventInjectionRequest(
                to,
                "test.complex.event",
                "Complex test event with nested structures",
                data
        );
    }
    
    /**
     * Create a batch of test events for load testing
     */
    public EventInjectionController.EventInjectionRequest[] createEventBatch(String to, int count) {
        EventInjectionController.EventInjectionRequest[] events = 
            new EventInjectionController.EventInjectionRequest[count];
        
        for (int i = 0; i < count; i++) {
            Map<String, Object> data = Map.of(
                    "batchId", UUID.randomUUID().toString(),
                    "eventIndex", i,
                    "totalEvents", count,
                    "userId", to,
                    "timestamp", Instant.now().toString()
            );
            
            events[i] = new EventInjectionController.EventInjectionRequest(
                    to,
                    "test.batch.event",
                    String.format("Batch event %d of %d", i + 1, count),
                    data
            );
        }
        
        return events;
    }
    
    /**
     * Create subscription request JSON for REST API calls
     */
    public String createSubscriptionRequestJson(String to, String endpoint, boolean enabled) throws JsonProcessingException {
        Map<String, Object> request = Map.of(
                "to", to,
                "deliveryMethod", "WEBHOOK",
                "endpoint", endpoint,
                "headers", Map.of(
                        "X-Test-Header", "api-test",
                        "Authorization", "Bearer test-token"
                ),
                "configuration", Map.of(),
                "enabled", enabled,
                "maxRetries", 3,
                "retryDelayMs", 1000,
                "batchSize", 1
        );
        
        return objectMapper.writeValueAsString(request);
    }
    
    /**
     * Create subscription request JSON with custom settings
     */
    public String createCustomSubscriptionRequestJson(
            String to, 
            String endpoint, 
            boolean enabled, 
            int maxRetries, 
            long retryDelayMs, 
            int batchSize) throws JsonProcessingException {
        
        Map<String, Object> request = Map.of(
                "to", to,
                "deliveryMethod", "WEBHOOK",
                "endpoint", endpoint,
                "headers", Map.of(
                        "X-Test-Header", "custom-test",
                        "Authorization", "Bearer test-token",
                        "X-Max-Retries", String.valueOf(maxRetries)
                ),
                "configuration", Map.of(
                        "timeout", 30000,
                        "customSetting", "testValue"
                ),
                "enabled", enabled,
                "maxRetries", maxRetries,
                "retryDelayMs", retryDelayMs,
                "batchSize", batchSize
        );
        
        return objectMapper.writeValueAsString(request);
    }
    
    /**
     * Create event injection request JSON
     */
    public String createEventInjectionRequestJson(EventInjectionController.EventInjectionRequest request) 
            throws JsonProcessingException {
        return objectMapper.writeValueAsString(request);
    }
    
    /**
     * Generate unique test user ID
     */
    public String generateTestUserId() {
        return "test-user-" + System.currentTimeMillis() + "-" + 
               UUID.randomUUID().toString().substring(0, 8);
    }
    
    /**
     * Generate unique test user ID with prefix
     */
    public String generateTestUserId(String prefix) {
        return prefix + "-" + System.currentTimeMillis() + "-" + 
               UUID.randomUUID().toString().substring(0, 8);
    }
    
    /**
     * Create multiple test users for batch testing
     */
    public String[] generateTestUsers(String prefix, int count) {
        String[] users = new String[count];
        for (int i = 0; i < count; i++) {
            users[i] = generateTestUserId(prefix + "-" + i);
        }
        return users;
    }
    
    /**
     * Create a test event for order processing scenario
     */
    public EventInjectionController.EventInjectionRequest createOrderEvent(String userId, String orderId) {
        Map<String, Object> orderData = Map.of(
                "orderId", orderId,
                "userId", userId,
                "amount", 99.99,
                "currency", "USD",
                "status", "created",
                "items", new Map[]{
                        Map.of("name", "Test Product", "quantity", 1, "price", 99.99)
                },
                "timestamp", Instant.now().toString()
        );
        
        return new EventInjectionController.EventInjectionRequest(
                userId,
                "order.created",
                "Order created: " + orderId,
                orderData
        );
    }
    
    /**
     * Create a test event for user profile update scenario
     */
    public EventInjectionController.EventInjectionRequest createUserUpdateEvent(String userId) {
        Map<String, Object> userData = Map.of(
                "userId", userId,
                "action", "profile_update",
                "changes", Map.of(
                        "email", userId + "@test.example.com",
                        "name", "Test User " + userId,
                        "lastLogin", Instant.now().toString()
                ),
                "timestamp", Instant.now().toString()
        );
        
        return new EventInjectionController.EventInjectionRequest(
                userId,
                "user.profile.updated",
                "User profile updated for " + userId,
                userData
        );
    }
    
    /**
     * Create a test event for notification scenario
     */
    public EventInjectionController.EventInjectionRequest createNotificationEvent(String userId, String message) {
        Map<String, Object> notificationData = Map.of(
                "userId", userId,
                "message", message,
                "type", "test_notification",
                "priority", "normal",
                "channels", new String[]{"webhook", "email"},
                "timestamp", Instant.now().toString()
        );
        
        return new EventInjectionController.EventInjectionRequest(
                userId,
                "notification.created",
                "Notification for " + userId,
                notificationData
        );
    }
    
    /**
     * Validate that a JSON string is well-formed
     */
    public boolean isValidJson(String json) {
        try {
            objectMapper.readTree(json);
            return true;
        } catch (JsonProcessingException e) {
            return false;
        }
    }
    
    /**
     * Pretty print JSON for debugging
     */
    public String prettyPrintJson(Object obj) throws JsonProcessingException {
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
    }
}