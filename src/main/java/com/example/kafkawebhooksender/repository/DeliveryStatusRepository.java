package com.example.kafkawebhooksender.repository;

import com.example.kafkawebhooksender.model.DeliveryStatus;
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
public class DeliveryStatusRepository {
    
    private static final Logger logger = LoggerFactory.getLogger(DeliveryStatusRepository.class);
    private static final String DELIVERY_STATUS_PREFIX = "delivery_status:";
    
    private final RocksDB rocksDB;
    private final ObjectMapper objectMapper;
    
    public DeliveryStatusRepository(RocksDB rocksDB, ObjectMapper objectMapper) {
        this.rocksDB = rocksDB;
        this.objectMapper = objectMapper;
    }
    
    public void save(DeliveryStatus deliveryStatus) throws RocksDBException {
        String key = DELIVERY_STATUS_PREFIX + deliveryStatus.getTo();
        try {
            byte[] value = objectMapper.writeValueAsBytes(deliveryStatus);
            rocksDB.put(key.getBytes(StandardCharsets.UTF_8), value);
            logger.debug("Saved delivery status for: {}", deliveryStatus.getTo());
        } catch (Exception e) {
            throw new RocksDBException("Failed to save delivery status: " + e.getMessage());
        }
    }
    
    public Optional<DeliveryStatus> findByTo(String to) {
        String key = DELIVERY_STATUS_PREFIX + to;
        try {
            byte[] value = rocksDB.get(key.getBytes(StandardCharsets.UTF_8));
            if (value == null) {
                return Optional.empty();
            }
            
            DeliveryStatus status = objectMapper.readValue(value, DeliveryStatus.class);
            return Optional.of(status);
        } catch (Exception e) {
            logger.error("Failed to retrieve delivery status for: {}", to, e);
            return Optional.empty();
        }
    }
    
    public DeliveryStatus findByToOrCreateInitial(String to) {
        return findByTo(to).orElse(DeliveryStatus.initial(to));
    }
    
    public List<DeliveryStatus> findAll() {
        List<DeliveryStatus> statuses = new ArrayList<>();
        
        try (RocksIterator iterator = rocksDB.newIterator()) {
            iterator.seek(DELIVERY_STATUS_PREFIX.getBytes(StandardCharsets.UTF_8));
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (!key.startsWith(DELIVERY_STATUS_PREFIX)) {
                    break;
                }
                
                try {
                    DeliveryStatus status = objectMapper.readValue(iterator.value(), DeliveryStatus.class);
                    statuses.add(status);
                } catch (Exception e) {
                    logger.error("Failed to deserialize delivery status with key: {}", key, e);
                }
                
                iterator.next();
            }
        } catch (Exception e) {
            logger.error("Failed to retrieve all delivery statuses", e);
        }
        
        return statuses;
    }
    
    public List<DeliveryStatus> findStatusesWithFailures() {
        return findAll().stream()
                .filter(status -> status.getConsecutiveFailures() > 0)
                .toList();
    }
    
    public void delete(String to) throws RocksDBException {
        String key = DELIVERY_STATUS_PREFIX + to;
        try {
            rocksDB.delete(key.getBytes(StandardCharsets.UTF_8));
            logger.debug("Deleted delivery status for: {}", to);
        } catch (Exception e) {
            throw new RocksDBException("Failed to delete delivery status: " + e.getMessage());
        }
    }
}