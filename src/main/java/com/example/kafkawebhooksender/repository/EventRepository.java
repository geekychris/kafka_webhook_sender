package com.example.kafkawebhooksender.repository;

import com.example.kafkawebhooksender.model.StoredEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.jackson.JsonFormat;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@Repository
public class EventRepository {
    
    private static final Logger logger = LoggerFactory.getLogger(EventRepository.class);
    private static final String EVENT_PREFIX = "event:";
    private static final String SEQUENCE_COUNTER_PREFIX = "seq:";
    
    private final RocksDB rocksDB;
    private final ObjectMapper objectMapper;
    private final AtomicLong sequenceGenerator = new AtomicLong();
    
    public EventRepository(RocksDB rocksDB, ObjectMapper objectMapper) {
        this.rocksDB = rocksDB;
        this.objectMapper = objectMapper.copy().registerModule(new JsonFormat().getCloudEventJacksonModule());
        initializeSequenceGenerator();
    }
    
    private void initializeSequenceGenerator() {
        try {
            // Find the highest sequence ID across all 'to' values
            long maxSequence = 0;
            try (RocksIterator iterator = rocksDB.newIterator()) {
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    String key = new String(iterator.key(), StandardCharsets.UTF_8);
                    if (key.startsWith(EVENT_PREFIX)) {
                        // Extract sequence from key format: "event:to:sequenceId"
                        String[] parts = key.split(":");
                        if (parts.length >= 3) {
                            try {
                                long sequence = Long.parseLong(parts[2]);
                                maxSequence = Math.max(maxSequence, sequence);
                            } catch (NumberFormatException e) {
                                logger.warn("Invalid sequence in key: {}", key);
                            }
                        }
                    }
                    iterator.next();
                }
            }
            sequenceGenerator.set(maxSequence);
            logger.info("Initialized sequence generator with max sequence: {}", maxSequence);
        } catch (Exception e) {
            logger.error("Failed to initialize sequence generator", e);
            sequenceGenerator.set(0);
        }
    }
    
    public StoredEvent store(String to, CloudEvent cloudEvent) throws RocksDBException {
        long sequenceId = sequenceGenerator.incrementAndGet();
        StoredEvent storedEvent = StoredEvent.create(to, sequenceId, cloudEvent);
        
        String eventKey = EVENT_PREFIX + to + ":" + String.format("%020d", sequenceId);
        
        try {
            byte[] value = objectMapper.writeValueAsBytes(storedEvent);
            rocksDB.put(eventKey.getBytes(StandardCharsets.UTF_8), value);
            
            // Update sequence counter for this 'to'
            String seqKey = SEQUENCE_COUNTER_PREFIX + to;
            rocksDB.put(seqKey.getBytes(StandardCharsets.UTF_8), 
                       Long.toString(sequenceId).getBytes(StandardCharsets.UTF_8));
            
            logger.debug("Stored event with key: {}", eventKey);
            return storedEvent;
        } catch (Exception e) {
            throw new RocksDBException("Failed to store event: " + e.getMessage());
        }
    }
    
    public Optional<StoredEvent> findByKey(String eventKey) {
        try {
            byte[] value = rocksDB.get((EVENT_PREFIX + eventKey).getBytes(StandardCharsets.UTF_8));
            if (value == null) {
                return Optional.empty();
            }
            
            StoredEvent event = objectMapper.readValue(value, StoredEvent.class);
            return Optional.of(event);
        } catch (Exception e) {
            logger.error("Failed to retrieve event with key: {}", eventKey, e);
            return Optional.empty();
        }
    }
    
    public List<StoredEvent> findEventsByToAfterSequence(String to, long afterSequenceId, int limit) {
        List<StoredEvent> events = new ArrayList<>();
        String startKey = EVENT_PREFIX + to + ":" + String.format("%020d", afterSequenceId + 1);
        String endKey = EVENT_PREFIX + to + ":" + "z"; // End boundary
        
        try (RocksIterator iterator = rocksDB.newIterator()) {
            iterator.seek(startKey.getBytes(StandardCharsets.UTF_8));
            
            int count = 0;
            while (iterator.isValid() && count < limit) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                
                // Check if we're still within the 'to' range
                if (!key.startsWith(EVENT_PREFIX + to + ":") || key.compareTo(endKey) >= 0) {
                    break;
                }
                
                try {
                    StoredEvent event = objectMapper.readValue(iterator.value(), StoredEvent.class);
                    events.add(event);
                    count++;
                } catch (Exception e) {
                    logger.error("Failed to deserialize event with key: {}", key, e);
                }
                
                iterator.next();
            }
        } catch (Exception e) {
            logger.error("Failed to find events for to: {} after sequence: {}", to, afterSequenceId, e);
        }
        
        return events;
    }
    
    public List<StoredEvent> findEventsByToInRange(String to, long fromSequenceId, long toSequenceId) {
        List<StoredEvent> events = new ArrayList<>();
        String startKey = EVENT_PREFIX + to + ":" + String.format("%020d", fromSequenceId);
        String endKey = EVENT_PREFIX + to + ":" + String.format("%020d", toSequenceId);
        
        try (RocksIterator iterator = rocksDB.newIterator()) {
            iterator.seek(startKey.getBytes(StandardCharsets.UTF_8));
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                
                if (key.compareTo(endKey) > 0) {
                    break;
                }
                
                if (!key.startsWith(EVENT_PREFIX + to + ":")) {
                    break;
                }
                
                try {
                    StoredEvent event = objectMapper.readValue(iterator.value(), StoredEvent.class);
                    events.add(event);
                } catch (Exception e) {
                    logger.error("Failed to deserialize event with key: {}", key, e);
                }
                
                iterator.next();
            }
        } catch (Exception e) {
            logger.error("Failed to find events for to: {} in range: {} - {}", to, fromSequenceId, toSequenceId, e);
        }
        
        return events;
    }
    
    public long getLatestSequenceId(String to) {
        try {
            String seqKey = SEQUENCE_COUNTER_PREFIX + to;
            byte[] value = rocksDB.get(seqKey.getBytes(StandardCharsets.UTF_8));
            if (value == null) {
                return -1;
            }
            return Long.parseLong(new String(value, StandardCharsets.UTF_8));
        } catch (Exception e) {
            logger.error("Failed to get latest sequence ID for to: {}", to, e);
            return -1;
        }
    }
    
    public long getEventCount(String to) {
        long count = 0;
        String prefix = EVENT_PREFIX + to + ":";
        
        try (RocksIterator iterator = rocksDB.newIterator()) {
            iterator.seek(prefix.getBytes(StandardCharsets.UTF_8));
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (!key.startsWith(prefix)) {
                    break;
                }
                count++;
                iterator.next();
            }
        } catch (Exception e) {
            logger.error("Failed to count events for to: {}", to, e);
        }
        
        return count;
    }
}