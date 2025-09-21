package com.example.kafkawebhooksender.repository;

import com.example.kafkawebhooksender.model.Subscription;
import com.fasterxml.jackson.databind.ObjectMapper;
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

@Repository
public class SubscriptionRepository {
    
    private static final Logger logger = LoggerFactory.getLogger(SubscriptionRepository.class);
    private static final String SUBSCRIPTION_PREFIX = "subscription:";
    
    private final RocksDB rocksDB;
    private final ObjectMapper objectMapper;
    
    public SubscriptionRepository(RocksDB rocksDB, ObjectMapper objectMapper) {
        this.rocksDB = rocksDB;
        this.objectMapper = objectMapper;
    }
    
    public void save(Subscription subscription) throws RocksDBException {
        String key = SUBSCRIPTION_PREFIX + subscription.getTo();
        try {
            byte[] value = objectMapper.writeValueAsBytes(subscription);
            rocksDB.put(key.getBytes(StandardCharsets.UTF_8), value);
            logger.debug("Saved subscription for: {}", subscription.getTo());
        } catch (Exception e) {
            throw new RocksDBException("Failed to save subscription: " + e.getMessage());
        }
    }
    
    public Optional<Subscription> findByTo(String to) {
        String key = SUBSCRIPTION_PREFIX + to;
        try {
            byte[] value = rocksDB.get(key.getBytes(StandardCharsets.UTF_8));
            if (value == null) {
                return Optional.empty();
            }
            
            Subscription subscription = objectMapper.readValue(value, Subscription.class);
            return Optional.of(subscription);
        } catch (Exception e) {
            logger.error("Failed to retrieve subscription for: {}", to, e);
            return Optional.empty();
        }
    }
    
    public List<Subscription> findAll() {
        List<Subscription> subscriptions = new ArrayList<>();
        
        try (RocksIterator iterator = rocksDB.newIterator()) {
            iterator.seek(SUBSCRIPTION_PREFIX.getBytes(StandardCharsets.UTF_8));
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (!key.startsWith(SUBSCRIPTION_PREFIX)) {
                    break;
                }
                
                try {
                    Subscription subscription = objectMapper.readValue(iterator.value(), Subscription.class);
                    subscriptions.add(subscription);
                } catch (Exception e) {
                    logger.error("Failed to deserialize subscription with key: {}", key, e);
                }
                
                iterator.next();
            }
        } catch (Exception e) {
            logger.error("Failed to retrieve all subscriptions", e);
        }
        
        return subscriptions;
    }
    
    public List<Subscription> findEnabledSubscriptions() {
        return findAll().stream()
                .filter(Subscription::isEnabled)
                .toList();
    }
    
    public boolean exists(String to) {
        String key = SUBSCRIPTION_PREFIX + to;
        try {
            byte[] value = rocksDB.get(key.getBytes(StandardCharsets.UTF_8));
            return value != null;
        } catch (Exception e) {
            logger.error("Failed to check subscription existence for: {}", to, e);
            return false;
        }
    }
    
    public void delete(String to) throws RocksDBException {
        String key = SUBSCRIPTION_PREFIX + to;
        try {
            rocksDB.delete(key.getBytes(StandardCharsets.UTF_8));
            logger.debug("Deleted subscription for: {}", to);
        } catch (Exception e) {
            throw new RocksDBException("Failed to delete subscription: " + e.getMessage());
        }
    }
    
    public long count() {
        long count = 0;
        
        try (RocksIterator iterator = rocksDB.newIterator()) {
            iterator.seek(SUBSCRIPTION_PREFIX.getBytes(StandardCharsets.UTF_8));
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (!key.startsWith(SUBSCRIPTION_PREFIX)) {
                    break;
                }
                count++;
                iterator.next();
            }
        } catch (Exception e) {
            logger.error("Failed to count subscriptions", e);
        }
        
        return count;
    }
}