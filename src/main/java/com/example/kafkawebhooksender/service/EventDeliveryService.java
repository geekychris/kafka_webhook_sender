package com.example.kafkawebhooksender.service;

import com.example.kafkawebhooksender.model.DeliveryStatus;
import com.example.kafkawebhooksender.model.StoredEvent;
import com.example.kafkawebhooksender.model.Subscription;
import com.example.kafkawebhooksender.repository.DeliveryStatusRepository;
import com.example.kafkawebhooksender.repository.EventRepository;
import com.example.kafkawebhooksender.repository.SubscriptionRepository;
import com.example.kafkawebhooksender.service.delivery.DeliveryService;
import com.example.kafkawebhooksender.service.delivery.DeliveryServiceRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class EventDeliveryService {
    
    private static final Logger logger = LoggerFactory.getLogger(EventDeliveryService.class);
    
    private final EventRepository eventRepository;
    private final SubscriptionRepository subscriptionRepository;
    private final DeliveryStatusRepository deliveryStatusRepository;
    private final DeliveryServiceRegistry deliveryServiceRegistry;
    private final ExecutorService deliveryExecutor;
    
    // Metrics
    private final Counter deliveryAttemptsCounter;
    private final Counter deliverySuccessCounter;
    private final Counter deliveryFailureCounter;
    private final AtomicLong pendingDeliveries = new AtomicLong(0);
    
    public EventDeliveryService(EventRepository eventRepository,
                               SubscriptionRepository subscriptionRepository,
                               DeliveryStatusRepository deliveryStatusRepository,
                               DeliveryServiceRegistry deliveryServiceRegistry,
                               MeterRegistry meterRegistry) {
        this.eventRepository = eventRepository;
        this.subscriptionRepository = subscriptionRepository;
        this.deliveryStatusRepository = deliveryStatusRepository;
        this.deliveryServiceRegistry = deliveryServiceRegistry;
        this.deliveryExecutor = Executors.newFixedThreadPool(10);
        
        // Initialize metrics
        this.deliveryAttemptsCounter = Counter.builder("event.delivery.attempts")
                .description("Number of event delivery attempts")
                .register(meterRegistry);
        
        this.deliverySuccessCounter = Counter.builder("event.delivery.success")
                .description("Number of successful event deliveries")
                .register(meterRegistry);
        
        this.deliveryFailureCounter = Counter.builder("event.delivery.failure")
                .description("Number of failed event deliveries")
                .register(meterRegistry);
        
        Gauge.builder("event.delivery.pending", this, EventDeliveryService::getPendingDeliveries)
                .description("Number of pending event deliveries")
                .register(meterRegistry);
    }
    
    @Scheduled(fixedDelayString = "${event.delivery.scheduler.delay:5000}")
    public void processDeliveries() {
        logger.debug("Starting delivery processing cycle");
        
        try {
            List<Subscription> enabledSubscriptions = subscriptionRepository.findEnabledSubscriptions();
            logger.debug("Found {} enabled subscriptions", enabledSubscriptions.size());
            
            // Process each subscription concurrently
            List<CompletableFuture<Void>> futures = enabledSubscriptions.stream()
                    .map(subscription -> CompletableFuture.runAsync(
                            () -> processSubscriptionDelivery(subscription),
                            deliveryExecutor
                    ))
                    .toList();
            
            // Wait for all delivery attempts to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            
        } catch (Exception e) {
            logger.error("Error during delivery processing cycle", e);
        }
        
        logger.debug("Completed delivery processing cycle");
    }
    
    private void processSubscriptionDelivery(Subscription subscription) {
        try {
            DeliveryStatus status = deliveryStatusRepository.findByToOrCreateInitial(subscription.getTo());
            
            // Check if we should retry failed deliveries
            if (status.getConsecutiveFailures() > 0 && 
                !status.shouldRetry(subscription.getMaxRetries(), subscription.getRetryDelayMs())) {
                logger.debug("Skipping delivery for {} - not ready for retry", subscription.getTo());
                return;
            }
            
            // Get the next batch of events to deliver
            long nextSequenceId = status.getLastDeliveredSequenceId() + 1;
            List<StoredEvent> events = eventRepository.findEventsByToAfterSequence(
                    subscription.getTo(), 
                    status.getLastDeliveredSequenceId(), 
                    subscription.getBatchSize()
            );
            
            if (events.isEmpty()) {
                logger.debug("No events to deliver for subscription: {}", subscription.getTo());
                return;
            }
            
            logger.debug("Attempting to deliver {} events for subscription: {}", events.size(), subscription.getTo());
            
            // Find appropriate delivery service
            Optional<DeliveryService> deliveryServiceOpt = deliveryServiceRegistry.getService(subscription.getDeliveryMethod());
            if (deliveryServiceOpt.isEmpty()) {
                logger.error("No delivery service found for method: {} for subscription: {}", 
                           subscription.getDeliveryMethod(), subscription.getTo());
                return;
            }
            
            DeliveryService deliveryService = deliveryServiceOpt.get();
            pendingDeliveries.incrementAndGet();
            
            try {
                deliveryAttemptsCounter.increment();
                
                // Attempt delivery
                DeliveryService.DeliveryResult result = deliveryService.deliver(subscription, events);
                
                if (result.success()) {
                    // Update delivery status with successful delivery
                    long deliveredSequenceId = result.deliveredSequenceId();
                    DeliveryStatus updatedStatus = status.withSuccessfulDelivery(deliveredSequenceId);
                    deliveryStatusRepository.save(updatedStatus);
                    
                    deliverySuccessCounter.increment();
                    logger.info("Successfully delivered {} events for subscription: {}, up to sequence: {}", 
                               events.size(), subscription.getTo(), deliveredSequenceId);
                } else {
                    // Update delivery status with failure
                    long attemptedSequenceId = events.get(events.size() - 1).getSequenceId();
                    DeliveryStatus updatedStatus = status.withFailedDelivery(attemptedSequenceId, result.message());
                    deliveryStatusRepository.save(updatedStatus);
                    
                    deliveryFailureCounter.increment();
                    logger.warn("Failed to deliver events for subscription: {}, error: {}", 
                               subscription.getTo(), result.message());
                }
                
            } finally {
                pendingDeliveries.decrementAndGet();
            }
            
        } catch (RocksDBException e) {
            logger.error("Database error during delivery processing for subscription: {}", subscription.getTo(), e);
        } catch (Exception e) {
            logger.error("Unexpected error during delivery processing for subscription: {}", subscription.getTo(), e);
        }
    }
    
    /**
     * Manually trigger delivery processing for a specific subscription
     */
    public void triggerDelivery(String to) {
        Optional<Subscription> subscription = subscriptionRepository.findByTo(to);
        if (subscription.isPresent() && subscription.get().isEnabled()) {
            CompletableFuture.runAsync(() -> processSubscriptionDelivery(subscription.get()), deliveryExecutor);
        } else {
            logger.warn("Cannot trigger delivery - subscription not found or not enabled for: {}", to);
        }
    }
    
    /**
     * Get statistics for a specific subscription
     */
    public DeliveryStatistics getDeliveryStatistics(String to) {
        DeliveryStatus status = deliveryStatusRepository.findByToOrCreateInitial(to);
        long totalEvents = eventRepository.getEventCount(to);
        long latestSequenceId = eventRepository.getLatestSequenceId(to);
        long pendingEvents = Math.max(0, latestSequenceId - status.getLastDeliveredSequenceId());
        
        return new DeliveryStatistics(
                to,
                totalEvents,
                status.getLastDeliveredSequenceId(),
                latestSequenceId,
                pendingEvents,
                status.getConsecutiveFailures(),
                status.getLastSuccessfulDelivery(),
                status.getLastError()
        );
    }
    
    public double getPendingDeliveries() {
        return pendingDeliveries.get();
    }
    
    /**
     * Statistics for a specific subscription's delivery status
     */
    public record DeliveryStatistics(
            String to,
            long totalEvents,
            long lastDeliveredSequenceId,
            long latestSequenceId,
            long pendingEvents,
            int consecutiveFailures,
            java.time.Instant lastSuccessfulDelivery,
            String lastError
    ) {}
}