package com.example.kafkawebhooksender.service;

import com.example.kafkawebhooksender.model.DeliveryMethod;
import com.example.kafkawebhooksender.model.StoredEvent;
import com.example.kafkawebhooksender.model.Subscription;
import com.example.kafkawebhooksender.service.delivery.DeliveryService;
import com.example.kafkawebhooksender.service.delivery.WebhookDeliveryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

class WebhookDeliveryServiceTest {
    
    private WebhookDeliveryService deliveryService;
    private WireMockServer wireMockServer;
    private ObjectMapper objectMapper;
    
    @BeforeEach
    void setUp() {
        wireMockServer = new WireMockServer(8090);
        wireMockServer.start();
        
        objectMapper = new ObjectMapper();
        WebClient.Builder webClientBuilder = WebClient.builder();
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        
        deliveryService = new WebhookDeliveryService(webClientBuilder, objectMapper, meterRegistry);
    }
    
    @AfterEach
    void tearDown() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }
    
    @Test
    void shouldSupportWebhookDeliveryMethod() {
        assertThat(deliveryService.supports(DeliveryMethod.WEBHOOK)).isTrue();
        assertThat(deliveryService.supports(DeliveryMethod.EMAIL)).isFalse();
    }
    
    @Test
    void shouldDeliverSingleEventSuccessfully() {
        // Given
        wireMockServer.stubFor(post(urlEqualTo("/webhook"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody("{\"status\":\"success\"}")));
        
        Subscription subscription = Subscription.builder()
                .to("test-user")
                .deliveryMethod(DeliveryMethod.WEBHOOK)
                .endpoint("http://localhost:8090/webhook")
                .headers(Map.of("Authorization", "Bearer token"))
                .build();
        
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("test://source"))
                .withType("test.event")
                .withData("application/json", "{\"message\":\"test\"}".getBytes())
                .build();
        
        StoredEvent storedEvent = StoredEvent.create("test-user", 1L, cloudEvent);
        
        // When
        DeliveryService.DeliveryResult result = deliveryService.deliver(subscription, List.of(storedEvent));
        
        // Then
        assertThat(result.success()).isTrue();
        assertThat(result.deliveredSequenceId()).isEqualTo(1L);
        
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/webhook"))
                .withHeader("Authorization", equalTo("Bearer token"))
                .withHeader("Content-Type", equalTo("application/json")));
    }
    
    @Test
    void shouldDeliverBatchEventAsArray() {
        // Given
        wireMockServer.stubFor(post(urlEqualTo("/webhook"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody("{\"status\":\"success\"}")));
        
        Subscription subscription = Subscription.builder()
                .to("batch-user")
                .deliveryMethod(DeliveryMethod.WEBHOOK)
                .endpoint("http://localhost:8090/webhook")
                .build();
        
        List<StoredEvent> events = List.of(
                StoredEvent.create("batch-user", 1L, createTestCloudEvent("event1")),
                StoredEvent.create("batch-user", 2L, createTestCloudEvent("event2")),
                StoredEvent.create("batch-user", 3L, createTestCloudEvent("event3"))
        );
        
        // When
        DeliveryService.DeliveryResult result = deliveryService.deliver(subscription, events);
        
        // Then
        assertThat(result.success()).isTrue();
        assertThat(result.deliveredSequenceId()).isEqualTo(3L);
        
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/webhook"))
                .withRequestBody(matchingJsonPath("$", withSize(3))));
    }
    
    @Test
    void shouldHandleDeliveryFailure() {
        // Given
        wireMockServer.stubFor(post(urlEqualTo("/webhook"))
                .willReturn(aResponse()
                        .withStatus(500)
                        .withBody("Internal Server Error")));
        
        Subscription subscription = Subscription.builder()
                .to("test-user")
                .deliveryMethod(DeliveryMethod.WEBHOOK)
                .endpoint("http://localhost:8090/webhook")
                .build();
        
        StoredEvent storedEvent = StoredEvent.create("test-user", 1L, createTestCloudEvent("test"));
        
        // When
        DeliveryService.DeliveryResult result = deliveryService.deliver(subscription, List.of(storedEvent));
        
        // Then
        assertThat(result.success()).isFalse();
        assertThat(result.httpStatusCode()).isEqualTo(500);
        assertThat(result.message()).contains("HTTP 500");
    }
    
    @Test
    void shouldReturnFailureForEmptyEventList() {
        // Given
        Subscription subscription = Subscription.builder()
                .to("test-user")
                .deliveryMethod(DeliveryMethod.WEBHOOK)
                .endpoint("http://localhost:8090/webhook")
                .build();
        
        // When
        DeliveryService.DeliveryResult result = deliveryService.deliver(subscription, List.of());
        
        // Then
        assertThat(result.success()).isFalse();
        assertThat(result.message()).contains("No events to deliver");
    }
    
    private CloudEvent createTestCloudEvent(String data) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("test://source"))
                .withType("test.event")
                .withData("application/json", ("{\"message\":\"" + data + "\"}").getBytes())
                .build();
    }
}