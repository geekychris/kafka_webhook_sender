package com.example.kafkawebhooksender.service.delivery;

import com.example.kafkawebhooksender.model.DeliveryMethod;
import com.example.kafkawebhooksender.model.StoredEvent;
import com.example.kafkawebhooksender.model.Subscription;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.jackson.JsonFormat;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Service
public class WebhookDeliveryService implements DeliveryService {
    
    private static final Logger logger = LoggerFactory.getLogger(WebhookDeliveryService.class);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    
    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final Counter deliverySuccessCounter;
    private final Counter deliveryFailureCounter;
    private final Timer deliveryTimer;
    
    public WebhookDeliveryService(WebClient.Builder webClientBuilder, 
                                 ObjectMapper objectMapper, 
                                 MeterRegistry meterRegistry) {
        this.webClient = webClientBuilder
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024)) // 10MB
                .build();
        this.objectMapper = objectMapper.copy().registerModule(new JsonFormat().getCloudEventJacksonModule());
        
        // Initialize metrics
        this.deliverySuccessCounter = Counter.builder("webhook.delivery")
                .description("Webhook delivery attempts")
                .tag("status", "success")
                .register(meterRegistry);
        
        this.deliveryFailureCounter = Counter.builder("webhook.delivery")
                .description("Webhook delivery attempts")
                .tag("status", "failure")
                .register(meterRegistry);
        
        this.deliveryTimer = Timer.builder("webhook.delivery.time")
                .description("Time taken for webhook delivery")
                .register(meterRegistry);
    }
    
    @Override
    public boolean supports(DeliveryMethod deliveryMethod) {
        return DeliveryMethod.WEBHOOK == deliveryMethod;
    }
    
    @Override
    public DeliveryResult deliver(Subscription subscription, List<StoredEvent> events) {
        if (events.isEmpty()) {
            return DeliveryResult.failure("No events to deliver");
        }
        
        Timer.Sample sample = Timer.start();
        
        try {
            logger.debug("Delivering {} events to webhook: {} for subscription: {}", 
                        events.size(), subscription.getEndpoint(), subscription.getTo());
            
            // Convert events to CloudEvents JSON batch format
            List<CloudEvent> cloudEvents = events.stream()
                    .map(StoredEvent::getCloudEvent)
                    .toList();
            
            String jsonBatch = createCloudEventsBatch(cloudEvents);
            
            // Build request with headers
            WebClient.RequestHeadersSpec<?> request = webClient.post()
                    .uri(subscription.getEndpoint())
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("Ce-Specversion", "1.0")
                    .bodyValue(jsonBatch);
            
            // Add custom headers from subscription
            for (Map.Entry<String, String> header : subscription.getHeaders().entrySet()) {
                request = request.header(header.getKey(), header.getValue());
            }
            
            // Execute the request
            String response = request
                    .retrieve()
                    .bodyToMono(String.class)
                    .timeout(REQUEST_TIMEOUT)
                    .block();
            
            long lastSequenceId = events.get(events.size() - 1).getSequenceId();
            
            logger.debug("Successfully delivered {} events to webhook: {}", events.size(), subscription.getEndpoint());
            deliverySuccessCounter.increment();
            return DeliveryResult.success("Delivered " + events.size() + " events", lastSequenceId);
            
        } catch (WebClientResponseException e) {
            logger.error("Webhook delivery failed with HTTP status: {} for endpoint: {}", 
                        e.getStatusCode(), subscription.getEndpoint(), e);
            deliveryFailureCounter.increment();
            return DeliveryResult.failure(
                    "HTTP " + e.getStatusCode().value() + ": " + e.getResponseBodyAsString(),
                    e.getStatusCode().value(),
                    e
            );
            
        } catch (Exception e) {
            logger.error("Webhook delivery failed for endpoint: {}", subscription.getEndpoint(), e);
            deliveryFailureCounter.increment();
            return DeliveryResult.failure("Delivery failed: " + e.getMessage(), e);
        } finally {
            sample.stop(deliveryTimer);
        }
    }
    
    private String createCloudEventsBatch(List<CloudEvent> events) throws Exception {
        if (events.size() == 1) {
            // Single event - send as individual CloudEvent
            return new String(new JsonFormat().serialize(events.get(0)));
        } else {
            // Multiple events - send as JSON batch
            // CloudEvents batch format is simply a JSON array of CloudEvents
            StringBuilder batchBuilder = new StringBuilder("[");
            for (int i = 0; i < events.size(); i++) {
                if (i > 0) {
                    batchBuilder.append(",");
                }
                batchBuilder.append(new String(new JsonFormat().serialize(events.get(i))));
            }
            batchBuilder.append("]");
            return batchBuilder.toString();
        }
    }
}