package com.example.kafkawebhooksender.controller;

import com.example.kafkawebhooksender.model.DeliveryStatus;
import com.example.kafkawebhooksender.model.Subscription;
import com.example.kafkawebhooksender.repository.DeliveryStatusRepository;
import com.example.kafkawebhooksender.repository.EventRepository;
import com.example.kafkawebhooksender.repository.SubscriptionRepository;
import com.example.kafkawebhooksender.service.EventDeliveryService;
import com.example.kafkawebhooksender.service.delivery.DeliveryServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/monitoring")
public class MonitoringController {
    
    private static final Logger logger = LoggerFactory.getLogger(MonitoringController.class);
    
    private final SubscriptionRepository subscriptionRepository;
    private final EventRepository eventRepository;
    private final DeliveryStatusRepository deliveryStatusRepository;
    private final EventDeliveryService eventDeliveryService;
    private final DeliveryServiceRegistry deliveryServiceRegistry;
    
    public MonitoringController(SubscriptionRepository subscriptionRepository,
                               EventRepository eventRepository,
                               DeliveryStatusRepository deliveryStatusRepository,
                               EventDeliveryService eventDeliveryService,
                               DeliveryServiceRegistry deliveryServiceRegistry) {
        this.subscriptionRepository = subscriptionRepository;
        this.eventRepository = eventRepository;
        this.deliveryStatusRepository = deliveryStatusRepository;
        this.eventDeliveryService = eventDeliveryService;
        this.deliveryServiceRegistry = deliveryServiceRegistry;
    }
    
    @GetMapping("/health")
    public ResponseEntity<HealthStatus> getHealth() {
        try {
            long subscriptionCount = subscriptionRepository.count();
            List<DeliveryStatus> failedStatuses = deliveryStatusRepository.findStatusesWithFailures();
            
            boolean healthy = failedStatuses.stream()
                    .noneMatch(status -> status.getConsecutiveFailures() >= 5);
            
            HealthStatus health = new HealthStatus(
                    healthy ? "UP" : "DEGRADED",
                    Instant.now(),
                    Map.of(
                            "subscriptions", subscriptionCount,
                            "failedDeliveries", failedStatuses.size(),
                            "supportedDeliveryMethods", deliveryServiceRegistry.getSupportedMethods()
                    )
            );
            
            return ResponseEntity.ok(health);
        } catch (Exception e) {
            logger.error("Failed to get health status", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new HealthStatus("DOWN", Instant.now(), Map.of("error", e.getMessage())));
        }
    }
    
    @GetMapping("/statistics")
    public ResponseEntity<SystemStatistics> getSystemStatistics() {
        try {
            List<Subscription> allSubscriptions = subscriptionRepository.findAll();
            List<DeliveryStatus> allStatuses = deliveryStatusRepository.findAll();
            
            long enabledSubscriptions = allSubscriptions.stream()
                    .mapToLong(sub -> sub.isEnabled() ? 1 : 0)
                    .sum();
            
            long totalEvents = allSubscriptions.stream()
                    .mapToLong(sub -> eventRepository.getEventCount(sub.getTo()))
                    .sum();
            
            long totalPendingEvents = allSubscriptions.stream()
                    .mapToLong(sub -> {
                        EventDeliveryService.DeliveryStatistics stats = eventDeliveryService.getDeliveryStatistics(sub.getTo());
                        return stats.pendingEvents();
                    })
                    .sum();
            
            long failedDeliveries = allStatuses.stream()
                    .mapToLong(status -> status.getConsecutiveFailures() > 0 ? 1 : 0)
                    .sum();
            
            SystemStatistics stats = new SystemStatistics(
                    allSubscriptions.size(),
                    enabledSubscriptions,
                    totalEvents,
                    totalPendingEvents,
                    failedDeliveries,
                    eventDeliveryService.getPendingDeliveries(),
                    Instant.now()
            );
            
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            logger.error("Failed to get system statistics", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    @GetMapping("/statistics/by-subscription")
    public ResponseEntity<List<EventDeliveryService.DeliveryStatistics>> getStatisticsBySubscription() {
        try {
            List<Subscription> subscriptions = subscriptionRepository.findAll();
            List<EventDeliveryService.DeliveryStatistics> stats = subscriptions.stream()
                    .map(sub -> eventDeliveryService.getDeliveryStatistics(sub.getTo()))
                    .toList();
            
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            logger.error("Failed to get statistics by subscription", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    @GetMapping("/failed-deliveries")
    public ResponseEntity<List<FailedDeliveryInfo>> getFailedDeliveries() {
        try {
            List<DeliveryStatus> failedStatuses = deliveryStatusRepository.findStatusesWithFailures();
            List<FailedDeliveryInfo> failedInfo = failedStatuses.stream()
                    .map(status -> {
                        Subscription subscription = subscriptionRepository.findByTo(status.getTo()).orElse(null);
                        return new FailedDeliveryInfo(
                                status.getTo(),
                                subscription != null ? subscription.getDeliveryMethod() : null,
                                subscription != null ? subscription.getEndpoint() : null,
                                status.getConsecutiveFailures(),
                                status.getLastError(),
                                status.getLastDeliveryAttempt(),
                                status.getLastSuccessfulDelivery()
                        );
                    })
                    .toList();
            
            return ResponseEntity.ok(failedInfo);
        } catch (Exception e) {
            logger.error("Failed to get failed deliveries", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    @GetMapping("/queue-status")
    public ResponseEntity<List<QueueStatus>> getQueueStatus() {
        try {
            List<Subscription> subscriptions = subscriptionRepository.findAll();
            List<QueueStatus> queueStatuses = subscriptions.stream()
                    .map(subscription -> {
                        EventDeliveryService.DeliveryStatistics stats = eventDeliveryService.getDeliveryStatistics(subscription.getTo());
                        return new QueueStatus(
                                subscription.getTo(),
                                subscription.isEnabled(),
                                stats.totalEvents(),
                                stats.pendingEvents(),
                                stats.lastDeliveredSequenceId(),
                                stats.latestSequenceId(),
                                stats.consecutiveFailures(),
                                stats.lastSuccessfulDelivery()
                        );
                    })
                    .toList();
            
            return ResponseEntity.ok(queueStatuses);
        } catch (Exception e) {
            logger.error("Failed to get queue status", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
    
    // Response DTOs
    public record HealthStatus(
            String status,
            Instant timestamp,
            Map<String, Object> details
    ) {}
    
    public record SystemStatistics(
            long totalSubscriptions,
            long enabledSubscriptions,
            long totalEvents,
            long totalPendingEvents,
            long failedDeliveries,
            double currentDeliveryTasks,
            Instant timestamp
    ) {}
    
    public record FailedDeliveryInfo(
            String to,
            com.example.kafkawebhooksender.model.DeliveryMethod deliveryMethod,
            String endpoint,
            int consecutiveFailures,
            String lastError,
            Instant lastAttempt,
            Instant lastSuccessfulDelivery
    ) {}
    
    public record QueueStatus(
            String to,
            boolean enabled,
            long totalEvents,
            long pendingEvents,
            long lastDeliveredSequenceId,
            long latestSequenceId,
            int consecutiveFailures,
            Instant lastSuccessfulDelivery
    ) {}
}