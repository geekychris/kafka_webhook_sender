package com.example.kafkawebhooksender.controller;

import com.example.kafkawebhooksender.model.DeliveryMethod;
import com.example.kafkawebhooksender.model.Subscription;
import com.example.kafkawebhooksender.repository.SubscriptionRepository;
import com.example.kafkawebhooksender.service.EventDeliveryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.rocksdb.RocksDBException;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class SubscriptionControllerTest {
    
    private MockMvc mockMvc;
    private ObjectMapper objectMapper;
    
    @Mock
    private SubscriptionRepository subscriptionRepository;
    
    @Mock
    private EventDeliveryService eventDeliveryService;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        objectMapper = new ObjectMapper();
        
        SubscriptionController controller = new SubscriptionController(
                subscriptionRepository,
                eventDeliveryService,
                new SimpleMeterRegistry()
        );
        
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }
    
    @Test
    void shouldCreateSubscriptionSuccessfully() throws Exception {
        // Given
        when(subscriptionRepository.exists("test-user")).thenReturn(false);
        
        SubscriptionController.CreateSubscriptionRequest request = 
                new SubscriptionController.CreateSubscriptionRequest(
                        "test-user",
                        DeliveryMethod.WEBHOOK,
                        "http://example.com/webhook",
                        Map.of("Authorization", "Bearer token"),
                        Map.of(),
                        true,
                        3,
                        1000L,
                        10
                );
        
        // When & Then
        mockMvc.perform(post("/api/subscriptions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.to").value("test-user"))
                .andExpect(jsonPath("$.deliveryMethod").value("WEBHOOK"))
                .andExpect(jsonPath("$.endpoint").value("http://example.com/webhook"));
        
        verify(subscriptionRepository).save(any(Subscription.class));
    }
    
    @Test
    void shouldReturnConflictWhenSubscriptionExists() throws Exception {
        // Given
        when(subscriptionRepository.exists("existing-user")).thenReturn(true);
        
        SubscriptionController.CreateSubscriptionRequest request = 
                new SubscriptionController.CreateSubscriptionRequest(
                        "existing-user",
                        DeliveryMethod.WEBHOOK,
                        "http://example.com/webhook",
                        Map.of(),
                        Map.of(),
                        true,
                        3,
                        1000L,
                        10
                );
        
        // When & Then
        mockMvc.perform(post("/api/subscriptions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isConflict());
        
        verify(subscriptionRepository, never()).save(any(Subscription.class));
    }
    
    @Test
    void shouldGetSubscriptionById() throws Exception {
        // Given
        Subscription subscription = Subscription.builder()
                .to("test-user")
                .deliveryMethod(DeliveryMethod.WEBHOOK)
                .endpoint("http://example.com/webhook")
                .build();
        
        when(subscriptionRepository.findByTo("test-user")).thenReturn(Optional.of(subscription));
        
        // When & Then
        mockMvc.perform(get("/api/subscriptions/test-user"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.to").value("test-user"))
                .andExpect(jsonPath("$.deliveryMethod").value("WEBHOOK"));
    }
    
    @Test
    void shouldReturnNotFoundWhenSubscriptionDoesNotExist() throws Exception {
        // Given
        when(subscriptionRepository.findByTo("nonexistent-user")).thenReturn(Optional.empty());
        
        // When & Then
        mockMvc.perform(get("/api/subscriptions/nonexistent-user"))
                .andExpect(status().isNotFound());
    }
    
    @Test
    void shouldGetAllSubscriptions() throws Exception {
        // Given
        List<Subscription> subscriptions = List.of(
                Subscription.builder()
                        .to("user1")
                        .deliveryMethod(DeliveryMethod.WEBHOOK)
                        .endpoint("http://example.com/webhook1")
                        .build(),
                Subscription.builder()
                        .to("user2")
                        .deliveryMethod(DeliveryMethod.WEBHOOK)
                        .endpoint("http://example.com/webhook2")
                        .build()
        );
        
        when(subscriptionRepository.findAll()).thenReturn(subscriptions);
        
        // When & Then
        mockMvc.perform(get("/api/subscriptions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].to").value("user1"))
                .andExpect(jsonPath("$[1].to").value("user2"));
    }
    
    @Test
    void shouldTriggerDelivery() throws Exception {
        // When & Then
        mockMvc.perform(post("/api/subscriptions/test-user/trigger-delivery"))
                .andExpect(status().isAccepted());
        
        verify(eventDeliveryService).triggerDelivery("test-user");
    }
    
    @Test
    void shouldGetDeliveryStatistics() throws Exception {
        // Given
        EventDeliveryService.DeliveryStatistics stats = 
                new EventDeliveryService.DeliveryStatistics(
                        "test-user",
                        10L,
                        8L,
                        10L,
                        2L,
                        0,
                        Instant.now(),
                        null
                );
        
        when(eventDeliveryService.getDeliveryStatistics("test-user")).thenReturn(stats);
        
        // When & Then
        mockMvc.perform(get("/api/subscriptions/test-user/statistics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.to").value("test-user"))
                .andExpect(jsonPath("$.totalEvents").value(10))
                .andExpect(jsonPath("$.pendingEvents").value(2));
    }
}