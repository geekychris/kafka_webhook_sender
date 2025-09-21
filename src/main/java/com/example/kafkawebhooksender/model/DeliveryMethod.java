package com.example.kafkawebhooksender.model;

/**
 * Supported delivery methods for event subscriptions
 */
public enum DeliveryMethod {
    WEBHOOK,
    EMAIL,
    SMS,
    SLACK,
    DISCORD
}