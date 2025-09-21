package com.example.kafkawebhooksender.controller;

import com.example.kafkawebhooksender.model.Subscription;
import com.example.kafkawebhooksender.repository.SubscriptionRepository;
import com.example.kafkawebhooksender.service.EventDeliveryService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.Valid;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/subscriptions")
public class SubscriptionController {
    
    private static final Logger logger = LoggerFactory.getLogger(SubscriptionController.class);
    
    private final SubscriptionRepository subscriptionRepository;
    private final EventDeliveryService eventDeliveryService;
    private final Counter subscriptionCreateCounter;
    private final Counter subscriptionUpdateCounter;
    private final Counter subscriptionDeleteCounter;
    
    public SubscriptionController(SubscriptionRepository subscriptionRepository,
                                 EventDeliveryService eventDeliveryService,
                                 MeterRegistry meterRegistry) {
        this.subscriptionRepository = subscriptionRepository;
        this.eventDeliveryService = eventDeliveryService;
        
        this.subscriptionCreateCounter = Counter.builder("subscription.create")
                .description("Number of subscriptions created")
                .register(meterRegistry);
        
        this.subscriptionUpdateCounter = Counter.builder("subscription.update")
                .description("Number of subscriptions updated")
                .register(meterRegistry);
        
        this.subscriptionDeleteCounter = Counter.builder("subscription.delete")
                .description("Number of subscriptions deleted")
                .register(meterRegistry);
    }
    
    @GetMapping
    public ResponseEntity<List<Subscription>> getAllSubscriptions() {
        try {
            List<Subscription> subscriptions = subscriptionRepository.findAll();
            return ResponseEntity.ok(subscriptions);
        } catch (Exception e) {
            logger.error("Failed to retrieve subscriptions", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    @GetMapping("/{to}")
    public ResponseEntity<Subscription> getSubscription(@PathVariable String to) {
        try {
            Optional<Subscription> subscription = subscriptionRepository.findByTo(to);
            return subscription.map(ResponseEntity::ok)
                             .orElse(ResponseEntity.notFound().build());
        } catch (Exception e) {
            logger.error("Failed to retrieve subscription for: {}", to, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    @PostMapping
    public ResponseEntity<Subscription> createSubscription(@Valid @RequestBody CreateSubscriptionRequest request) {
        try {
            // Check if subscription already exists
            if (subscriptionRepository.exists(request.to())) {
                return ResponseEntity.status(HttpStatus.CONFLICT)
                        .header("X-Error-Message", "Subscription already exists for: " + request.to())
                        .build();
            }
            
            Subscription subscription = Subscription.builder()
                    .to(request.to())
                    .deliveryMethod(request.deliveryMethod())
                    .endpoint(request.endpoint())
                    .headers(request.headers() != null ? request.headers() : new java.util.HashMap<>())
                    .configuration(request.configuration() != null ? request.configuration() : new java.util.HashMap<>())
                    .enabled(request.enabled() != null ? request.enabled() : true)
                    .maxRetries(request.maxRetries() != null ? request.maxRetries() : 3)
                    .retryDelayMs(request.retryDelayMs() != null ? request.retryDelayMs() : 1000L)
                    .batchSize(request.batchSize() != null ? request.batchSize() : 1)
                    .build();
            
            subscriptionRepository.save(subscription);
            subscriptionCreateCounter.increment();
            
            logger.info("Created subscription for: {}", request.to());
            return ResponseEntity.status(HttpStatus.CREATED).body(subscription);
            
        } catch (RocksDBException e) {
            logger.error("Failed to create subscription for: {}", request.to(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    @PutMapping("/{to}")
    public ResponseEntity<Subscription> updateSubscription(@PathVariable String to, 
                                                          @Valid @RequestBody UpdateSubscriptionRequest request) {
        try {
            Optional<Subscription> existingSubscription = subscriptionRepository.findByTo(to);
            if (existingSubscription.isEmpty()) {
                return ResponseEntity.notFound().build();
            }
            
            Subscription updated = existingSubscription.get().withUpdates(
                    request.deliveryMethod(),
                    request.endpoint(),
                    request.headers(),
                    request.configuration(),
                    request.enabled(),
                    request.maxRetries(),
                    request.retryDelayMs(),
                    request.batchSize()
            );
            
            subscriptionRepository.save(updated);
            subscriptionUpdateCounter.increment();
            
            logger.info("Updated subscription for: {}", to);
            return ResponseEntity.ok(updated);
            
        } catch (RocksDBException e) {
            logger.error("Failed to update subscription for: {}", to, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    @DeleteMapping("/{to}")
    public ResponseEntity<Void> deleteSubscription(@PathVariable String to) {
        try {
            if (!subscriptionRepository.exists(to)) {
                return ResponseEntity.notFound().build();
            }
            
            subscriptionRepository.delete(to);
            subscriptionDeleteCounter.increment();
            
            logger.info("Deleted subscription for: {}", to);
            return ResponseEntity.noContent().build();
            
        } catch (RocksDBException e) {
            logger.error("Failed to delete subscription for: {}", to, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    @PostMapping("/{to}/trigger-delivery")
    public ResponseEntity<Void> triggerDelivery(@PathVariable String to) {
        try {
            eventDeliveryService.triggerDelivery(to);
            return ResponseEntity.accepted().build();
        } catch (Exception e) {
            logger.error("Failed to trigger delivery for: {}", to, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    @GetMapping("/{to}/statistics")
    public ResponseEntity<EventDeliveryService.DeliveryStatistics> getDeliveryStatistics(@PathVariable String to) {
        try {
            EventDeliveryService.DeliveryStatistics stats = eventDeliveryService.getDeliveryStatistics(to);
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            logger.error("Failed to get delivery statistics for: {}", to, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    // Request/Response DTOs
    public record CreateSubscriptionRequest(
            @jakarta.validation.constraints.NotBlank String to,
            @jakarta.validation.constraints.NotNull com.example.kafkawebhooksender.model.DeliveryMethod deliveryMethod,
            @jakarta.validation.constraints.NotBlank String endpoint,
            java.util.Map<String, String> headers,
            java.util.Map<String, Object> configuration,
            Boolean enabled,
            Integer maxRetries,
            Long retryDelayMs,
            Integer batchSize
    ) {}
    
    public record UpdateSubscriptionRequest(
            com.example.kafkawebhooksender.model.DeliveryMethod deliveryMethod,
            String endpoint,
            java.util.Map<String, String> headers,
            java.util.Map<String, Object> configuration,
            Boolean enabled,
            Integer maxRetries,
            Long retryDelayMs,
            Integer batchSize
    ) {}
}