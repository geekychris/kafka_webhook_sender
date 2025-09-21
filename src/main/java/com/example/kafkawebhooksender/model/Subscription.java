package com.example.kafkawebhooksender.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a subscription for event delivery
 */
public class Subscription {
    
    @NotBlank
    private final String to;
    
    @NotNull
    private final DeliveryMethod deliveryMethod;
    
    @NotBlank
    private final String endpoint;
    
    private final Map<String, String> headers;
    private final Map<String, Object> configuration;
    private final boolean enabled;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final int maxRetries;
    private final long retryDelayMs;
    private final int batchSize;
    
    @JsonCreator
    public Subscription(
            @JsonProperty("to") String to,
            @JsonProperty("deliveryMethod") DeliveryMethod deliveryMethod,
            @JsonProperty("endpoint") String endpoint,
            @JsonProperty("headers") Map<String, String> headers,
            @JsonProperty("configuration") Map<String, Object> configuration,
            @JsonProperty("enabled") Boolean enabled,
            @JsonProperty("createdAt") Instant createdAt,
            @JsonProperty("updatedAt") Instant updatedAt,
            @JsonProperty("maxRetries") Integer maxRetries,
            @JsonProperty("retryDelayMs") Long retryDelayMs,
            @JsonProperty("batchSize") Integer batchSize) {
        this.to = to;
        this.deliveryMethod = deliveryMethod;
        this.endpoint = endpoint;
        this.headers = headers != null ? Map.copyOf(headers) : Map.of();
        this.configuration = configuration != null ? Map.copyOf(configuration) : Map.of();
        this.enabled = enabled != null ? enabled : true;
        this.createdAt = createdAt != null ? createdAt : Instant.now();
        this.updatedAt = updatedAt != null ? updatedAt : Instant.now();
        this.maxRetries = maxRetries != null ? maxRetries : 10;
        this.retryDelayMs = retryDelayMs != null ? retryDelayMs : 1000L;
        this.batchSize = batchSize != null ? batchSize : 10;
    }
    
    // Builder pattern for easier construction
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String to;
        private DeliveryMethod deliveryMethod;
        private String endpoint;
        private Map<String, String> headers = Map.of();
        private Map<String, Object> configuration = Map.of();
        private Boolean enabled = true;
        private Instant createdAt;
        private Instant updatedAt;
        private Integer maxRetries = 10;
        private Long retryDelayMs = 1000L;
        private Integer batchSize = 10;
        
        public Builder to(String to) {
            this.to = to;
            return this;
        }
        
        public Builder deliveryMethod(DeliveryMethod deliveryMethod) {
            this.deliveryMethod = deliveryMethod;
            return this;
        }
        
        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }
        
        public Builder headers(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }
        
        public Builder configuration(Map<String, Object> configuration) {
            this.configuration = configuration;
            return this;
        }
        
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }
        
        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }
        
        public Builder retryDelayMs(long retryDelayMs) {
            this.retryDelayMs = retryDelayMs;
            return this;
        }
        
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }
        
        public Subscription build() {
            return new Subscription(to, deliveryMethod, endpoint, headers, configuration,
                    enabled, createdAt, updatedAt, maxRetries, retryDelayMs, batchSize);
        }
    }
    
    // Copy constructor for updates
    public Subscription withUpdates(DeliveryMethod deliveryMethod, String endpoint, 
                                  Map<String, String> headers, Map<String, Object> configuration,
                                  Boolean enabled, Integer maxRetries, Long retryDelayMs, Integer batchSize) {
        return new Subscription(
                this.to,
                deliveryMethod != null ? deliveryMethod : this.deliveryMethod,
                endpoint != null ? endpoint : this.endpoint,
                headers != null ? headers : this.headers,
                configuration != null ? configuration : this.configuration,
                enabled != null ? enabled : this.enabled,
                this.createdAt,
                Instant.now(),
                maxRetries != null ? maxRetries : this.maxRetries,
                retryDelayMs != null ? retryDelayMs : this.retryDelayMs,
                batchSize != null ? batchSize : this.batchSize
        );
    }
    
    // Getters
    public String getTo() { return to; }
    public DeliveryMethod getDeliveryMethod() { return deliveryMethod; }
    public String getEndpoint() { return endpoint; }
    public Map<String, String> getHeaders() { return headers; }
    public Map<String, Object> getConfiguration() { return configuration; }
    public boolean isEnabled() { return enabled; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public int getMaxRetries() { return maxRetries; }
    public long getRetryDelayMs() { return retryDelayMs; }
    public int getBatchSize() { return batchSize; }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Subscription that)) return false;
        return enabled == that.enabled &&
                maxRetries == that.maxRetries &&
                retryDelayMs == that.retryDelayMs &&
                batchSize == that.batchSize &&
                Objects.equals(to, that.to) &&
                deliveryMethod == that.deliveryMethod &&
                Objects.equals(endpoint, that.endpoint) &&
                Objects.equals(headers, that.headers) &&
                Objects.equals(configuration, that.configuration) &&
                Objects.equals(createdAt, that.createdAt) &&
                Objects.equals(updatedAt, that.updatedAt);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(to, deliveryMethod, endpoint, headers, configuration, 
                           enabled, createdAt, updatedAt, maxRetries, retryDelayMs, batchSize);
    }
    
    @Override
    public String toString() {
        return "Subscription{" +
                "to='" + to + '\'' +
                ", deliveryMethod=" + deliveryMethod +
                ", endpoint='" + endpoint + '\'' +
                ", enabled=" + enabled +
                ", maxRetries=" + maxRetries +
                ", retryDelayMs=" + retryDelayMs +
                ", batchSize=" + batchSize +
                '}';
    }
}