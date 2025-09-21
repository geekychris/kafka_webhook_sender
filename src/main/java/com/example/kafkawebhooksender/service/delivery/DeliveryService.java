package com.example.kafkawebhooksender.service.delivery;

import com.example.kafkawebhooksender.model.DeliveryMethod;
import com.example.kafkawebhooksender.model.StoredEvent;
import com.example.kafkawebhooksender.model.Subscription;

import java.util.List;

/**
 * Interface for delivery services that can send events via different methods
 */
public interface DeliveryService {
    
    /**
     * Checks if this service supports the given delivery method
     */
    boolean supports(DeliveryMethod deliveryMethod);
    
    /**
     * Delivers a batch of events to a subscription endpoint
     * 
     * @param subscription The subscription containing delivery details
     * @param events The events to deliver
     * @return DeliveryResult indicating success or failure
     */
    DeliveryResult deliver(Subscription subscription, List<StoredEvent> events);
    
    /**
     * Result of a delivery attempt
     */
    record DeliveryResult(
            boolean success,
            String message,
            int httpStatusCode,
            long deliveredSequenceId,
            Exception exception
    ) {
        public static DeliveryResult success(long deliveredSequenceId) {
            return new DeliveryResult(true, "Delivered successfully", 200, deliveredSequenceId, null);
        }
        
        public static DeliveryResult success(String message, long deliveredSequenceId) {
            return new DeliveryResult(true, message, 200, deliveredSequenceId, null);
        }
        
        public static DeliveryResult failure(String message) {
            return new DeliveryResult(false, message, 0, -1, null);
        }
        
        public static DeliveryResult failure(String message, int httpStatusCode) {
            return new DeliveryResult(false, message, httpStatusCode, -1, null);
        }
        
        public static DeliveryResult failure(String message, Exception exception) {
            return new DeliveryResult(false, message, 0, -1, exception);
        }
        
        public static DeliveryResult failure(String message, int httpStatusCode, Exception exception) {
            return new DeliveryResult(false, message, httpStatusCode, -1, exception);
        }
    }
}