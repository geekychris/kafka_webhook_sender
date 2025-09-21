package com.example.kafkawebhooksender.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * Tracks delivery progress for each subscription's virtual queue
 */
public class DeliveryStatus {
    
    private final String to;
    private final long lastDeliveredSequenceId;
    private final long lastAttemptedSequenceId;
    private final int consecutiveFailures;
    private final Instant lastDeliveryAttempt;
    private final Instant lastSuccessfulDelivery;
    private final String lastError;
    
    @JsonCreator
    public DeliveryStatus(
            @JsonProperty("to") String to,
            @JsonProperty("lastDeliveredSequenceId") long lastDeliveredSequenceId,
            @JsonProperty("lastAttemptedSequenceId") long lastAttemptedSequenceId,
            @JsonProperty("consecutiveFailures") int consecutiveFailures,
            @JsonProperty("lastDeliveryAttempt") Instant lastDeliveryAttempt,
            @JsonProperty("lastSuccessfulDelivery") Instant lastSuccessfulDelivery,
            @JsonProperty("lastError") String lastError) {
        this.to = to;
        this.lastDeliveredSequenceId = lastDeliveredSequenceId;
        this.lastAttemptedSequenceId = lastAttemptedSequenceId;
        this.consecutiveFailures = consecutiveFailures;
        this.lastDeliveryAttempt = lastDeliveryAttempt;
        this.lastSuccessfulDelivery = lastSuccessfulDelivery;
        this.lastError = lastError;
    }
    
    public static DeliveryStatus initial(String to) {
        return new DeliveryStatus(to, -1, -1, 0, null, null, null);
    }
    
    public DeliveryStatus withSuccessfulDelivery(long sequenceId) {
        return new DeliveryStatus(
                to,
                sequenceId,
                sequenceId,
                0,
                Instant.now(),
                Instant.now(),
                null
        );
    }
    
    public DeliveryStatus withFailedDelivery(long sequenceId, String error) {
        return new DeliveryStatus(
                to,
                lastDeliveredSequenceId,
                sequenceId,
                consecutiveFailures + 1,
                Instant.now(),
                lastSuccessfulDelivery,
                error
        );
    }
    
    public boolean shouldRetry(int maxRetries, long retryDelayMs) {
        if (consecutiveFailures == 0) return false;
        if (consecutiveFailures >= maxRetries) return false;
        if (lastDeliveryAttempt == null) return true;
        
        long timeSinceLastAttempt = Instant.now().toEpochMilli() - lastDeliveryAttempt.toEpochMilli();
        long backoffDelay = calculateBackoffDelay(consecutiveFailures, retryDelayMs);
        return timeSinceLastAttempt >= backoffDelay;
    }
    
    private long calculateBackoffDelay(int attempts, long baseDelayMs) {
        // Exponential backoff with jitter
        long backoff = baseDelayMs * (1L << Math.min(attempts - 1, 10));
        return backoff + (long) (Math.random() * backoff * 0.1);
    }
    
    // Getters
    public String getTo() { return to; }
    public long getLastDeliveredSequenceId() { return lastDeliveredSequenceId; }
    public long getLastAttemptedSequenceId() { return lastAttemptedSequenceId; }
    public int getConsecutiveFailures() { return consecutiveFailures; }
    public Instant getLastDeliveryAttempt() { return lastDeliveryAttempt; }
    public Instant getLastSuccessfulDelivery() { return lastSuccessfulDelivery; }
    public String getLastError() { return lastError; }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeliveryStatus that)) return false;
        return lastDeliveredSequenceId == that.lastDeliveredSequenceId &&
                lastAttemptedSequenceId == that.lastAttemptedSequenceId &&
                consecutiveFailures == that.consecutiveFailures &&
                Objects.equals(to, that.to) &&
                Objects.equals(lastDeliveryAttempt, that.lastDeliveryAttempt) &&
                Objects.equals(lastSuccessfulDelivery, that.lastSuccessfulDelivery) &&
                Objects.equals(lastError, that.lastError);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(to, lastDeliveredSequenceId, lastAttemptedSequenceId, 
                           consecutiveFailures, lastDeliveryAttempt, lastSuccessfulDelivery, lastError);
    }
    
    @Override
    public String toString() {
        return "DeliveryStatus{" +
                "to='" + to + '\'' +
                ", lastDeliveredSequenceId=" + lastDeliveredSequenceId +
                ", lastAttemptedSequenceId=" + lastAttemptedSequenceId +
                ", consecutiveFailures=" + consecutiveFailures +
                ", lastDeliveryAttempt=" + lastDeliveryAttempt +
                ", lastSuccessfulDelivery=" + lastSuccessfulDelivery +
                ", lastError='" + lastError + '\'' +
                '}';
    }
}