package com.example.kafkawebhooksender.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.cloudevents.CloudEvent;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents a stored event in RocksDB with ordering information
 */
public class StoredEvent {
    
    private final String to;
    private final long sequenceId;
    private final CloudEvent cloudEvent;
    private final Instant receivedAt;
    private final String eventKey;
    
    @JsonCreator
    public StoredEvent(
            @JsonProperty("to") String to,
            @JsonProperty("sequenceId") long sequenceId,
            @JsonProperty("cloudEvent") CloudEvent cloudEvent,
            @JsonProperty("receivedAt") Instant receivedAt,
            @JsonProperty("eventKey") String eventKey) {
        this.to = to;
        this.sequenceId = sequenceId;
        this.cloudEvent = cloudEvent;
        this.receivedAt = receivedAt != null ? receivedAt : Instant.now();
        this.eventKey = eventKey;
    }
    
    public static StoredEvent create(String to, long sequenceId, CloudEvent cloudEvent) {
        String eventKey = generateEventKey(to, sequenceId);
        return new StoredEvent(to, sequenceId, cloudEvent, Instant.now(), eventKey);
    }
    
    public static String generateEventKey(String to, long sequenceId) {
        return String.format("%s:%020d", to, sequenceId);
    }
    
    // Getters
    public String getTo() { return to; }
    public long getSequenceId() { return sequenceId; }
    public CloudEvent getCloudEvent() { return cloudEvent; }
    public Instant getReceivedAt() { return receivedAt; }
    public String getEventKey() { return eventKey; }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StoredEvent that)) return false;
        return sequenceId == that.sequenceId &&
                Objects.equals(to, that.to) &&
                Objects.equals(cloudEvent, that.cloudEvent) &&
                Objects.equals(receivedAt, that.receivedAt) &&
                Objects.equals(eventKey, that.eventKey);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(to, sequenceId, cloudEvent, receivedAt, eventKey);
    }
    
    @Override
    public String toString() {
        return "StoredEvent{" +
                "to='" + to + '\'' +
                ", sequenceId=" + sequenceId +
                ", eventKey='" + eventKey + '\'' +
                ", receivedAt=" + receivedAt +
                '}';
    }
}