import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;

public class CreateSubscription {
    public static void main(String[] args) throws Exception {
        // Load RocksDB native library
        RocksDB.loadLibrary();
        
        String dbPath = "/app/data/rocksdb";
        
        try (final Options options = new Options().setCreateIfMissing(true);
             final RocksDB db = RocksDB.open(options, dbPath)) {
            
            // Create subscription JSON
            Map<String, Object> subscription = new HashMap<>();
            subscription.put("to", "user-events");
            subscription.put("deliveryMethod", "WEBHOOK");
            subscription.put("endpoint", "http://test-webhook:8080/webhook");
            subscription.put("enabled", true);
            subscription.put("maxRetries", 3);
            subscription.put("retryDelayMs", 1000);
            subscription.put("batchSize", 1);
            
            Map<String, String> headers = new HashMap<>();
            headers.put("Content-Type", "application/json");
            headers.put("X-Source", "kafka-webhook-sender");
            subscription.put("headers", headers);
            
            subscription.put("configuration", new HashMap<>());
            
            // Serialize to JSON
            ObjectMapper mapper = new ObjectMapper();
            String jsonValue = mapper.writeValueAsString(subscription);
            
            // Store in RocksDB with key pattern: subscription:user-events
            String key = "subscription:user-events";
            db.put(key.getBytes(), jsonValue.getBytes());
            
            System.out.println("Created subscription: " + key);
            System.out.println("Value: " + jsonValue);
        }
    }
}