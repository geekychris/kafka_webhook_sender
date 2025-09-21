package com.example.kafkawebhooksender.integration;

import com.example.kafkawebhooksender.KafkaWebhookSenderApplication;
import com.example.kafkawebhooksender.controller.SubscriptionController;
import com.example.kafkawebhooksender.model.DeliveryMethod;
import com.example.kafkawebhooksender.model.Subscription;
import com.example.kafkawebhooksender.repository.SubscriptionRepository;
import com.example.kafkawebhooksender.service.EventDeliveryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(
        classes = KafkaWebhookSenderApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@EmbeddedKafka(
        partitions = 1,
        brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"},
        topics = {"events"}
)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "rocksdb.path=./test-data/rocksdb",
        "event.delivery.scheduler.delay=1000",
        "logging.level.com.example.kafkawebhooksender=DEBUG"
})
class KafkaWebhookIntegrationTest {
    
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;
    
    @Autowired
    private TestRestTemplate restTemplate;
    
    @Autowired
    private SubscriptionRepository subscriptionRepository;
    
    @Autowired
    private EventDeliveryService eventDeliveryService;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @LocalServerPort
    private int port;
    
    private WireMockServer wireMockServer;
    private KafkaProducer<String, String> producer;
    
    @BeforeEach
    void setUp() {
        // Start WireMock server
        wireMockServer = new WireMockServer(8089);
        wireMockServer.start();
        
        // Configure Kafka producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer = new KafkaProducer<>(producerProps);
    }
    
    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
        }
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }
    
    @Test
    void shouldProcessKafkaEventAndDeliverToWebhook() throws Exception {
        // Given: Create a subscription
        String webhookUrl = "http://localhost:8089/webhook";
        SubscriptionController.CreateSubscriptionRequest subscriptionRequest = 
                new SubscriptionController.CreateSubscriptionRequest(
                        "user123",
                        DeliveryMethod.WEBHOOK,
                        webhookUrl,
                        Map.of("Authorization", "Bearer test-token"),
                        Map.of(),
                        true,
                        3,
                        1000L,
                        5
                );
        
        ResponseEntity<Subscription> createResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/subscriptions",
                subscriptionRequest,
                Subscription.class
        );
        
        assertThat(createResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        
        // Set up webhook mock
        wireMockServer.stubFor(post(urlEqualTo("/webhook"))
                .withHeader("Authorization", equalTo("Bearer test-token"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"status\":\"success\"}")));
        
        // When: Send a CloudEvent to Kafka
        CloudEvent event = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("test://source"))
                .withType("test.event")
                .withSubject("user123") // This will be used as 'to' field
                .withData("application/json", "{\"message\":\"Hello World\"}".getBytes())
                .withExtension("to", "user123") // Explicit 'to' field
                .build();
        
        String eventJson = new String(JsonFormat.INSTANCE.serialize(event));
        producer.send(new ProducerRecord<>("events", event.getId(), eventJson)).get();
        
        // Then: Wait for event to be processed and delivered
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    wireMockServer.verify(1, postRequestedFor(urlEqualTo("/webhook"))
                            .withHeader("Authorization", equalTo("Bearer test-token"))
                            .withHeader("Content-Type", equalTo("application/json")));
                });
        
        // Verify delivery statistics
        ResponseEntity<EventDeliveryService.DeliveryStatistics> statsResponse = restTemplate.getForEntity(
                "http://localhost:" + port + "/api/subscriptions/user123/statistics",
                EventDeliveryService.DeliveryStatistics.class
        );
        
        assertThat(statsResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        EventDeliveryService.DeliveryStatistics stats = statsResponse.getBody();
        assertThat(stats).isNotNull();
        assertThat(stats.totalEvents()).isEqualTo(1);
        assertThat(stats.pendingEvents()).isEqualTo(0);
        assertThat(stats.consecutiveFailures()).isEqualTo(0);
    }
    
    @Test
    void shouldRetryFailedDeliveries() throws Exception {
        // Given: Create a subscription
        String webhookUrl = "http://localhost:8089/failing-webhook";
        SubscriptionController.CreateSubscriptionRequest subscriptionRequest = 
                new SubscriptionController.CreateSubscriptionRequest(
                        "user456",
                        DeliveryMethod.WEBHOOK,
                        webhookUrl,
                        Map.of(),
                        Map.of(),
                        true,
                        3,
                        1000L,
                        1
                );
        
        restTemplate.postForEntity(
                "http://localhost:" + port + "/api/subscriptions",
                subscriptionRequest,
                Subscription.class
        );
        
        // Set up webhook mock to fail initially, then succeed
        wireMockServer.stubFor(post(urlEqualTo("/failing-webhook"))
                .inScenario("Retry Scenario")
                .whenScenarioStateIs("Started")
                .willReturn(aResponse()
                        .withStatus(500)
                        .withBody("Internal Server Error"))
                .willSetStateTo("First Failure"));
        
        wireMockServer.stubFor(post(urlEqualTo("/failing-webhook"))
                .inScenario("Retry Scenario")
                .whenScenarioStateIs("First Failure")
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody("{\"status\":\"success\"}")));
        
        // When: Send a CloudEvent to Kafka
        CloudEvent event = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("test://source"))
                .withType("test.event")
                .withExtension("to", "user456")
                .withData("application/json", "{\"message\":\"Test retry\"}".getBytes())
                .build();
        
        String eventJson = new String(JsonFormat.INSTANCE.serialize(event));
        producer.send(new ProducerRecord<>("events", event.getId(), eventJson)).get();
        
        // Then: Wait for event to be retried and eventually succeed
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    wireMockServer.verify(atLeast(2), postRequestedFor(urlEqualTo("/failing-webhook")));
                });
    }
    
    @Test
    void shouldHandleBatchDelivery() throws Exception {
        // Given: Create a subscription with batch size
        String webhookUrl = "http://localhost:8089/batch-webhook";
        SubscriptionController.CreateSubscriptionRequest subscriptionRequest = 
                new SubscriptionController.CreateSubscriptionRequest(
                        "batch-user",
                        DeliveryMethod.WEBHOOK,
                        webhookUrl,
                        Map.of(),
                        Map.of(),
                        true,
                        3,
                        1000L,
                        3
                );
        
        restTemplate.postForEntity(
                "http://localhost:" + port + "/api/subscriptions",
                subscriptionRequest,
                Subscription.class
        );
        
        // Set up webhook mock
        wireMockServer.stubFor(post(urlEqualTo("/batch-webhook"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody("{\"status\":\"success\"}")));
        
        // When: Send multiple CloudEvents
        for (int i = 0; i < 3; i++) {
            CloudEvent event = CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withSource(URI.create("test://source"))
                    .withType("test.event")
                    .withExtension("to", "batch-user")
                    .withData("application/json", ("{\"message\":\"Batch event " + i + "\"}").getBytes())
                    .build();
            
            String eventJson = new String(JsonFormat.INSTANCE.serialize(event));
            producer.send(new ProducerRecord<>("events", event.getId(), eventJson)).get();
        }
        
        // Then: Wait for batch delivery
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    wireMockServer.verify(1, postRequestedFor(urlEqualTo("/batch-webhook")));
                });
        
        // Verify the request body contains an array of events
        wireMockServer.verify(postRequestedFor(urlEqualTo("/batch-webhook"))
                .withRequestBody(matchingJsonPath("$", withSize(3))));
    }
    
    @Test
    void shouldProvideMonitoringEndpoints() {
        // Test health endpoint
        ResponseEntity<String> healthResponse = restTemplate.getForEntity(
                "http://localhost:" + port + "/api/monitoring/health",
                String.class
        );
        assertThat(healthResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        
        // Test statistics endpoint
        ResponseEntity<String> statsResponse = restTemplate.getForEntity(
                "http://localhost:" + port + "/api/monitoring/statistics",
                String.class
        );
        assertThat(statsResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        
        // Test Prometheus metrics endpoint
        ResponseEntity<String> metricsResponse = restTemplate.getForEntity(
                "http://localhost:" + port + "/actuator/prometheus",
                String.class
        );
        assertThat(metricsResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
    }
}