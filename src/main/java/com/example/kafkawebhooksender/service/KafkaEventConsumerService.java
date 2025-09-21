package com.example.kafkawebhooksender.service;

import com.example.kafkawebhooksender.model.StoredEvent;
import com.example.kafkawebhooksender.repository.EventRepository;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.jackson.JsonFormat;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class KafkaEventConsumerService {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaEventConsumerService.class);
    
    private final EventRepository eventRepository;
    private final EventFormat eventFormat;
    private final Counter eventsReceivedCounter;
    private final Counter eventsStoredCounter;
    private final Counter eventsErrorCounter;
    private final Timer eventProcessingTimer;
    
    public KafkaEventConsumerService(EventRepository eventRepository, MeterRegistry meterRegistry) {
        this.eventRepository = eventRepository;
        this.eventFormat = new JsonFormat();
        
        // Initialize metrics
        this.eventsReceivedCounter = Counter.builder("kafka.events.received")
                .description("Number of events received from Kafka")
                .register(meterRegistry);
        
        this.eventsStoredCounter = Counter.builder("kafka.events.stored")
                .description("Number of events successfully stored")
                .tag("status", "success")
                .register(meterRegistry);
        
        this.eventsErrorCounter = Counter.builder("kafka.events.stored")
                .description("Number of events that failed to be stored")
                .tag("status", "error")
                .register(meterRegistry);
        
        this.eventProcessingTimer = Timer.builder("kafka.event.processing.time")
                .description("Time taken to process and store events")
                .register(meterRegistry);
    }
    
    @KafkaListener(
            topics = "${kafka.topic.events:events}",
            groupId = "${kafka.consumer.group-id:webhook-sender-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeEvent(
            @Payload String eventData,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {
        
        Timer.Sample sample = Timer.start();
        eventsReceivedCounter.increment();
        long offset = record.offset();
        
        try {
            logger.debug("Received event from topic: {}, partition: {}, offset: {}", topic, partition, offset);
            
            // Parse CloudEvent from JSON
            CloudEvent cloudEvent = eventFormat.deserialize(eventData.getBytes());
            
            // Extract the 'to' field from CloudEvent
            String to = extractToField(cloudEvent);
            if (to == null || to.trim().isEmpty()) {
                logger.warn("Event missing 'to' field, skipping. Event ID: {}", cloudEvent.getId());
                eventsErrorCounter.increment();
                acknowledgment.acknowledge();
                return;
            }
            
            // Store the event
            StoredEvent storedEvent = eventRepository.store(to, cloudEvent);
            
            logger.debug("Stored event for '{}' with sequence ID: {}", to, storedEvent.getSequenceId());
            eventsStoredCounter.increment();
            
            // Acknowledge successful processing
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            logger.error("Failed to process event from topic: {}, partition: {}, offset: {}", 
                        topic, partition, offset, e);
            eventsErrorCounter.increment();
            
            // In a production scenario, you might want to handle this differently
            // For now, we'll acknowledge to avoid getting stuck on a bad message
            acknowledgment.acknowledge();
        } finally {
            sample.stop(eventProcessingTimer);
        }
    }
    
    private String extractToField(CloudEvent cloudEvent) {
        // Try to get 'to' from extension attributes first
        Object toExtension = cloudEvent.getExtension("to");
        if (toExtension != null) {
            return toExtension.toString();
        }
        
        // If not found in extensions, try to parse from data if it's JSON
        if (cloudEvent.getData() != null) {
            try {
                String dataStr = new String(cloudEvent.getData().toBytes());
                // Simple JSON parsing to find "to" field
                // In a production system, you might use a JSON parser
                if (dataStr.contains("\"to\"")) {
                    int start = dataStr.indexOf("\"to\"");
                    int colonIndex = dataStr.indexOf(":", start);
                    if (colonIndex > 0) {
                        int valueStart = dataStr.indexOf("\"", colonIndex) + 1;
                        int valueEnd = dataStr.indexOf("\"", valueStart);
                        if (valueEnd > valueStart) {
                            return dataStr.substring(valueStart, valueEnd);
                        }
                    }
                }
            } catch (Exception e) {
                logger.debug("Failed to extract 'to' from event data", e);
            }
        }
        
        // Try subject as fallback
        String subject = cloudEvent.getSubject();
        if (subject != null && !subject.trim().isEmpty()) {
            return subject;
        }
        
        logger.warn("Could not extract 'to' field from CloudEvent. ID: {}, Source: {}", 
                   cloudEvent.getId(), cloudEvent.getSource());
        return null;
    }
}