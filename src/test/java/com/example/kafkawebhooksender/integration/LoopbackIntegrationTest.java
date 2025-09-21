package com.example.kafkawebhooksender.integration;

import com.example.kafkawebhooksender.controller.EventInjectionController;
import com.example.kafkawebhooksender.controller.LoopbackWebhookController;
import com.example.kafkawebhooksender.model.Subscription;
import com.example.kafkawebhooksender.repository.SubscriptionRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "event.delivery.scheduler.delay=1000"  // Faster processing for testing
})
public class LoopbackIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private SubscriptionRepository subscriptionRepository;

    @Autowired
    private ObjectMapper objectMapper;

    private String baseUrl;
    private String testUserId;

    @BeforeEach
    void setUp() throws Exception {
        baseUrl = "http://localhost:" + port;
        testUserId = "integration-test-user-" + System.currentTimeMillis();
        
        // Clear any existing data
        try {
            subscriptionRepository.delete(testUserId);
        } catch (Exception e) {
            // Ignore if doesn't exist
        }
        
        // Clear loopback events
        restTemplate.delete(baseUrl + "/api/loopback/events");
    }

    @Test
    void testCompleteLoopbackWorkflow() throws Exception {
        // 1. Create loopback subscription
        Subscription subscription = createLoopbackSubscription();
        assertThat(subscription.to()).isEqualTo(testUserId);
        assertThat(subscription.enabled()).isTrue();
        
        // 2. Inject test event directly
        EventInjectionController.EventInjectionResponse injectionResponse = injectTestEvent();
        assertThat(injectionResponse.status()).isEqualTo("success");
        assertThat(injectionResponse.to()).isEqualTo(testUserId);
        assertThat(injectionResponse.method()).isEqualTo("direct");
        
        // 3. Wait for event processing and delivery
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ResponseEntity<String> response = restTemplate.getForEntity(
                baseUrl + "/api/loopback/events", String.class);
            
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
            
            JsonNode events = objectMapper.readTree(response.getBody());
            assertThat(events.size()).isGreaterThan(0);
            
            // Verify event content
            JsonNode firstEvent = events.elements().next();
            assertThat(firstEvent.get("eventId")).isNotNull();
            assertThat(firstEvent.get("payload")).isNotNull();
            assertThat(firstEvent.get("receivedAt")).isNotNull();
        });
        
        // 4. Verify loopback status
        LoopbackWebhookController.LoopbackStatus status = getLoopbackStatus();
        assertThat(status.active()).isTrue();
        assertThat(status.totalEventsReceived()).isGreaterThan(0);
        assertThat(status.eventCounter()).isGreaterThan(0);
        assertThat(status.lastEventReceived()).isNotNull();
    }

    @Test
    void testKafkaInjectionWorkflow() throws Exception {
        // 1. Create loopback subscription
        createLoopbackSubscription();
        
        // 2. Inject event via Kafka
        EventInjectionController.EventInjectionResponse injectionResponse = injectEventViaKafka();
        assertThat(injectionResponse.status()).isEqualTo("success");
        assertThat(injectionResponse.method()).isEqualTo("kafka");
        
        // 3. Wait for Kafka consumption and delivery
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            ResponseEntity<String> response = restTemplate.getForEntity(
                baseUrl + "/api/loopback/events", String.class);
            
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
            
            JsonNode events = objectMapper.readTree(response.getBody());
            assertThat(events.size()).isGreaterThan(0);
        });
    }

    @Test
    void testMultipleEventsAndBatching() throws Exception {
        // 1. Create subscription with batch size > 1
        createLoopbackSubscriptionWithBatching(3);
        
        // 2. Inject multiple events
        for (int i = 0; i < 5; i++) {
            injectTestEvent();
        }
        
        // 3. Wait for processing
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            ResponseEntity<String> response = restTemplate.getForEntity(
                baseUrl + "/api/loopback/events", String.class);
            
            JsonNode events = objectMapper.readTree(response.getBody());
            assertThat(events.size()).isGreaterThanOrEqualTo(3); // At least one batch should be delivered
        });
    }

    @Test
    void testDisabledSubscription() throws Exception {
        // 1. Create disabled subscription
        createDisabledLoopbackSubscription();
        
        // 2. Inject event
        injectTestEvent();
        
        // 3. Wait and verify no events are delivered
        Thread.sleep(5000); // Wait longer than normal processing time
        
        ResponseEntity<String> response = restTemplate.getForEntity(
            baseUrl + "/api/loopback/events", String.class);
        
        JsonNode events = objectMapper.readTree(response.getBody());
        assertThat(events.size()).isEqualTo(0); // No events should be delivered
        
        // 4. Enable subscription and trigger delivery
        enableSubscription();
        triggerDelivery();
        
        // 5. Now events should be delivered
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ResponseEntity<String> eventsResponse = restTemplate.getForEntity(
                baseUrl + "/api/loopback/events", String.class);
            
            JsonNode deliveredEvents = objectMapper.readTree(eventsResponse.getBody());
            assertThat(deliveredEvents.size()).isGreaterThan(0);
        });
    }

    @Test
    void testTestingStatusEndpoint() throws Exception {
        // 1. Get initial status
        EventInjectionController.TestingStatus initialStatus = getTestingStatus();
        assertThat(initialStatus.active()).isTrue();
        
        long initialDirectCount = initialStatus.directInjectionsCount();
        long initialKafkaCount = initialStatus.kafkaInjectionsCount();
        
        // 2. Create subscription and inject events
        createLoopbackSubscription();
        injectTestEvent(); // Direct
        injectEventViaKafka(); // Kafka
        
        // 3. Check updated status
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            EventInjectionController.TestingStatus updatedStatus = getTestingStatus();
            
            assertThat(updatedStatus.directInjectionsCount()).isGreaterThan(initialDirectCount);
            assertThat(updatedStatus.kafkaInjectionsCount()).isGreaterThan(initialKafkaCount);
            assertThat(updatedStatus.totalEvents()).isGreaterThan(0);
        });
    }

    @Test
    void testSimpleTestEventCreation() throws Exception {
        // 1. Create subscription
        createLoopbackSubscription();
        
        // 2. Use the simple test event creation endpoint
        ResponseEntity<EventInjectionController.EventInjectionResponse> response = 
            restTemplate.postForEntity(
                baseUrl + "/api/testing/create-test-event/" + testUserId + "?method=direct",
                null,
                EventInjectionController.EventInjectionResponse.class
            );
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().status()).isEqualTo("success");
        
        // 3. Verify delivery
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            LoopbackWebhookController.LoopbackStatus status = getLoopbackStatus();
            assertThat(status.totalEventsReceived()).isGreaterThan(0);
        });
    }

    // Helper methods

    private Subscription createLoopbackSubscription() {
        String endpoint = baseUrl + "/api/loopback/webhook";
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        
        String requestBody = String.format("""
            {
                "to": "%s",
                "deliveryMethod": "WEBHOOK",
                "endpoint": "%s",
                "headers": {
                    "X-Test-Header": "integration-test",
                    "Authorization": "Bearer test-token"
                },
                "configuration": {},
                "enabled": true,
                "maxRetries": 3,
                "retryDelayMs": 1000,
                "batchSize": 1
            }
            """, testUserId, endpoint);
        
        HttpEntity<String> request = new HttpEntity<>(requestBody, headers);
        
        ResponseEntity<Subscription> response = restTemplate.postForEntity(
            baseUrl + "/api/subscriptions", request, Subscription.class);
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return response.getBody();
    }

    private Subscription createLoopbackSubscriptionWithBatching(int batchSize) {
        String endpoint = baseUrl + "/api/loopback/webhook";
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        
        String requestBody = String.format("""
            {
                "to": "%s",
                "deliveryMethod": "WEBHOOK",
                "endpoint": "%s",
                "enabled": true,
                "maxRetries": 3,
                "batchSize": %d
            }
            """, testUserId, endpoint, batchSize);
        
        HttpEntity<String> request = new HttpEntity<>(requestBody, headers);
        
        ResponseEntity<Subscription> response = restTemplate.postForEntity(
            baseUrl + "/api/subscriptions", request, Subscription.class);
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return response.getBody();
    }

    private Subscription createDisabledLoopbackSubscription() {
        String endpoint = baseUrl + "/api/loopback/webhook";
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        
        String requestBody = String.format("""
            {
                "to": "%s",
                "deliveryMethod": "WEBHOOK",
                "endpoint": "%s",
                "enabled": false,
                "maxRetries": 3,
                "batchSize": 1
            }
            """, testUserId, endpoint);
        
        HttpEntity<String> request = new HttpEntity<>(requestBody, headers);
        
        ResponseEntity<Subscription> response = restTemplate.postForEntity(
            baseUrl + "/api/subscriptions", request, Subscription.class);
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return response.getBody();
    }

    private EventInjectionController.EventInjectionResponse injectTestEvent() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        
        String requestBody = String.format("""
            {
                "to": "%s",
                "eventType": "integration.test",
                "description": "Integration test event",
                "data": {
                    "testId": "%s",
                    "testType": "integration",
                    "timestamp": "%s",
                    "customData": {
                        "key1": "value1",
                        "key2": 42,
                        "nested": {
                            "prop": "nested-value"
                        }
                    }
                }
            }
            """, testUserId, testUserId, System.currentTimeMillis());
        
        HttpEntity<String> request = new HttpEntity<>(requestBody, headers);
        
        ResponseEntity<EventInjectionController.EventInjectionResponse> response = 
            restTemplate.postForEntity(
                baseUrl + "/api/testing/inject-direct",
                request,
                EventInjectionController.EventInjectionResponse.class
            );
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return response.getBody();
    }

    private EventInjectionController.EventInjectionResponse injectEventViaKafka() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        
        String requestBody = String.format("""
            {
                "to": "%s",
                "eventType": "integration.kafka.test",
                "description": "Integration test event via Kafka",
                "data": {
                    "testId": "%s",
                    "testType": "kafka-integration",
                    "timestamp": "%s",
                    "kafkaSpecific": true
                }
            }
            """, testUserId, testUserId, System.currentTimeMillis());
        
        HttpEntity<String> request = new HttpEntity<>(requestBody, headers);
        
        ResponseEntity<EventInjectionController.EventInjectionResponse> response = 
            restTemplate.postForEntity(
                baseUrl + "/api/testing/inject-kafka",
                request,
                EventInjectionController.EventInjectionResponse.class
            );
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return response.getBody();
    }

    private void enableSubscription() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        
        String requestBody = """
            {
                "enabled": true
            }
            """;
        
        HttpEntity<String> request = new HttpEntity<>(requestBody, headers);
        
        ResponseEntity<Subscription> response = restTemplate.exchange(
            baseUrl + "/api/subscriptions/" + testUserId,
            HttpMethod.PUT,
            request,
            Subscription.class
        );
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    private void triggerDelivery() {
        ResponseEntity<EventInjectionController.DeliveryTriggerResponse> response = 
            restTemplate.postForEntity(
                baseUrl + "/api/testing/trigger-delivery/" + testUserId,
                null,
                EventInjectionController.DeliveryTriggerResponse.class
            );
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    private LoopbackWebhookController.LoopbackStatus getLoopbackStatus() {
        ResponseEntity<LoopbackWebhookController.LoopbackStatus> response = 
            restTemplate.getForEntity(
                baseUrl + "/api/loopback/status",
                LoopbackWebhookController.LoopbackStatus.class
            );
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return response.getBody();
    }

    private EventInjectionController.TestingStatus getTestingStatus() {
        ResponseEntity<EventInjectionController.TestingStatus> response = 
            restTemplate.getForEntity(
                baseUrl + "/api/testing/status",
                EventInjectionController.TestingStatus.class
            );
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return response.getBody();
    }
}