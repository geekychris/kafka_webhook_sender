package com.example.kafkawebhooksender.service.delivery;

import com.example.kafkawebhooksender.model.DeliveryMethod;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
public class DeliveryServiceRegistry {
    
    private final List<DeliveryService> deliveryServices;
    
    public DeliveryServiceRegistry(List<DeliveryService> deliveryServices) {
        this.deliveryServices = deliveryServices;
    }
    
    public Optional<DeliveryService> getService(DeliveryMethod deliveryMethod) {
        return deliveryServices.stream()
                .filter(service -> service.supports(deliveryMethod))
                .findFirst();
    }
    
    public List<DeliveryMethod> getSupportedMethods() {
        return deliveryServices.stream()
                .map(service -> {
                    // This is a bit hacky, but we'll return all enum values that are supported
                    for (DeliveryMethod method : DeliveryMethod.values()) {
                        if (service.supports(method)) {
                            return method;
                        }
                    }
                    return null;
                })
                .filter(java.util.Objects::nonNull)
                .distinct()
                .toList();
    }
}