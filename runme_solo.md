# Kafka Webhook Sender - Solo Setup Guide

This guide shows you how to run the Kafka Webhook Sender locally and test it using the built-in loopback webhook consumer.

## Overview

The loopback webhook system allows you to:
1. **Create subscriptions** that point to the built-in loopback webhook endpoint
2. **Inject events** into the system (directly or via Kafka)
3. **Receive events** at the loopback webhook and inspect the delivered payloads

## Prerequisites

- Java 21
- Docker & Docker Compose (for Kafka infrastructure)
- `curl` and `jq` (for testing)

## Quick Start

### Step 1: Start Infrastructure Services

Start only the required infrastructure (Kafka, Zookeeper, test webhook):

```bash
docker-compose up zookeeper kafka test-webhook -d
```

Wait for services to be ready:
```bash
# Check that Kafka is ready
docker logs kafka | grep "started (kafka.server.KafkaServer)"

# Should see: "started (kafka.server.KafkaServer)"
```

### Step 2: Start the Application

Run the application locally:

```bash
# Start the application
SPRING_PROFILES_ACTIVE=dev java -jar target/kafka-webhook-sender-0.0.1-SNAPSHOT.jar
```

The application will start on **http://localhost:8090**

Wait for the application to fully start. You should see:
```
Started KafkaWebhookSenderApplication in X.XXX seconds
=== Controller Beans ===
Found controller bean: testController
Found controller bean: subscriptionController  
Found controller bean: loopbackWebhookController
Found controller bean: eventInjectionController
=========================
```

### Step 3: Verify Everything is Working

Check that all services are running:

```bash
# Check application health
curl -s http://localhost:8090/actuator/health | jq .

# Check loopback status  
curl -s http://localhost:8090/api/loopback/status | jq .

# Check testing endpoints
curl -s http://localhost:8090/api/testing/status | jq .
```

All should return healthy status.

## Setting Up Loopback Testing

### Step 4: Create a Loopback Subscription

Create a subscription that points to the built-in loopback webhook:

```bash
# Set your test user ID
TEST_USER="my-test-user"

# Create loopback subscription
curl -X POST http://localhost:8090/api/subscriptions \
  -H "Content-Type: application/json" \
  -d "{
    \"to\": \"$TEST_USER\",
    \"deliveryMethod\": \"WEBHOOK\",
    \"endpoint\": \"http://localhost:8090/api/loopback/webhook\",
    \"headers\": {
      \"X-Test-Header\": \"loopback-test\",
      \"Authorization\": \"Bearer test-token\"
    },
    \"enabled\": true,
    \"maxRetries\": 3,
    \"retryDelayMs\": 1000,
    \"batchSize\": 1
  }"
```

**Expected Response:**
```json
{
  "to": "my-test-user",
  "deliveryMethod": "WEBHOOK", 
  "endpoint": "http://localhost:8090/api/loopback/webhook",
  "enabled": true,
  "createdAt": "2025-09-20T23:13:01.123Z",
  "maxRetries": 3,
  "retryDelayMs": 1000,
  "batchSize": 1
}
```

### Step 5: Send Events to Your Subscription

#### Option A: Quick Test Event

Use the simple test event creator:

```bash
# Inject a simple test event
curl -X POST http://localhost:8090/api/testing/create-test-event/$TEST_USER
```

**Expected Response:**
```json
{
  "status": "success",
  "message": "Event injected directly into processing pipeline",
  "eventId": "12345-67890-abcdef",
  "to": "my-test-user",
  "method": "direct",
  "timestamp": "2025-09-20T23:13:01.456Z"
}
```

#### Option B: Custom Event (Direct Injection)

Inject a custom event directly into the processing pipeline:

```bash
curl -X POST http://localhost:8090/api/testing/inject-direct \
  -H "Content-Type: application/json" \
  -d "{
    \"to\": \"$TEST_USER\",
    \"eventType\": \"order.created\",
    \"description\": \"New order created\",
    \"data\": {
      \"orderId\": \"ORDER-123\",
      \"userId\": \"$TEST_USER\",
      \"amount\": 99.99,
      \"currency\": \"USD\",
      \"items\": [
        {\"name\": \"Test Product\", \"quantity\": 1, \"price\": 99.99}
      ],
      \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
    }
  }"
```

#### Option C: Send Event via Kafka

Send an event through the complete Kafka workflow:

```bash
curl -X POST http://localhost:8090/api/testing/inject-kafka \
  -H "Content-Type: application/json" \
  -d "{
    \"to\": \"$TEST_USER\",
    \"eventType\": \"user.profile.updated\",
    \"description\": \"User profile update\",
    \"data\": {
      \"userId\": \"$TEST_USER\",
      \"action\": \"profile_update\",
      \"changes\": {
        \"email\": \"user@example.com\",
        \"name\": \"Test User\"
      },
      \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
    }
  }"
```

### Step 6: Verify Event Delivery

The system processes events every **5 seconds**. Wait a moment, then check if your event was delivered:

```bash
# Wait for processing
sleep 8

# Check if events were received by the loopback webhook
curl -s http://localhost:8090/api/loopback/events | jq .
```

**Expected Response:**
```json
{
  "event-1": {
    "eventId": "event-1",
    "payload": "{\"specversion\":\"1.0\",\"id\":\"12345...\",\"data\":{...}}",
    "headers": {
      "content-type": "application/json",
      "X-Test-Header": "loopback-test",
      "Authorization": "Bearer test-token"
    },
    "receivedAt": "2025-09-20T23:13:08.789Z"
  }
}
```

### Step 7: Check Status and Statistics

Monitor your testing progress:

```bash
# Check loopback webhook status
curl -s http://localhost:8090/api/loopback/status | jq .

# Check testing statistics  
curl -s http://localhost:8090/api/testing/status | jq .

# Check overall system health
curl -s http://localhost:8090/api/monitoring/health | jq .
```

## Understanding the Workflow

Here's what happens when you send an event:

1. **Event Injection** → Event is stored in RocksDB with a sequence ID
2. **Background Processing** → Every 5 seconds, the system checks for pending events  
3. **Event Delivery** → Events are sent as HTTP POST to webhook endpoints
4. **Loopback Reception** → The built-in loopback webhook receives and stores events
5. **Inspection** → You can view received events via the loopback API

## Advanced Testing Scenarios

### Multiple Users

Test with multiple subscriptions:

```bash
# Create subscriptions for multiple users
for user in user-a user-b user-c; do
  curl -X POST http://localhost:8090/api/subscriptions \
    -H "Content-Type: application/json" \
    -d "{
      \"to\": \"$user\",
      \"deliveryMethod\": \"WEBHOOK\",
      \"endpoint\": \"http://localhost:8090/api/loopback/webhook\",
      \"enabled\": true,
      \"maxRetries\": 3,
      \"batchSize\": 1
    }"
done

# Send events to all users
for user in user-a user-b user-c; do
  curl -X POST http://localhost:8090/api/testing/create-test-event/$user
done
```

### Batch Processing

Test event batching by creating a subscription with `batchSize > 1`:

```bash
curl -X POST http://localhost:8090/api/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "to": "batch-test-user",
    "deliveryMethod": "WEBHOOK", 
    "endpoint": "http://localhost:8090/api/loopback/webhook",
    "enabled": true,
    "batchSize": 5
  }'

# Send multiple events quickly
for i in {1..5}; do
  curl -X POST http://localhost:8090/api/testing/create-test-event/batch-test-user &
done
wait
```

### Manual Delivery Trigger

If you don't want to wait for the 5-second cycle:

```bash
# Manually trigger delivery for a specific user
curl -X POST http://localhost:8090/api/testing/trigger-delivery/$TEST_USER
```

## Troubleshooting

### Events Not Being Delivered

1. **Check subscription exists and is enabled:**
   ```bash
   curl -s http://localhost:8090/api/subscriptions/$TEST_USER | jq '.enabled'
   ```

2. **Check for pending events:**
   ```bash
   curl -s http://localhost:8090/api/testing/status | jq '.totalEvents'
   ```

3. **Manually trigger delivery:**
   ```bash
   curl -X POST http://localhost:8090/api/testing/trigger-delivery/$TEST_USER
   ```

4. **Check application logs** for any errors

### Loopback Not Receiving Events

1. **Verify loopback is active:**
   ```bash
   curl -s http://localhost:8090/api/loopback/status | jq '.active'
   ```

2. **Test loopback directly:**
   ```bash
   curl -X POST http://localhost:8090/api/loopback/webhook \
     -H "Content-Type: application/json" \
     -d '{"test": "direct webhook call"}'
   ```

3. **Check subscription endpoint URL** matches the loopback URL exactly

### Clear Test Data

Reset your testing environment:

```bash
# Clear loopback events
curl -X DELETE http://localhost:8090/api/loopback/events

# Delete a test subscription
curl -X DELETE http://localhost:8090/api/subscriptions/$TEST_USER
```

## Automated Testing

For comprehensive testing, run the automated end-to-end test:

```bash
# Run automated test suite
BASE_URL=http://localhost:8090 ./test-e2e.sh

# Run with verbose output for debugging  
BASE_URL=http://localhost:8090 ./test-e2e.sh --verbose
```

This will test:
- ✅ Service connectivity
- ✅ Subscription creation
- ✅ Event injection (direct and Kafka)
- ✅ Event processing and delivery  
- ✅ Loopback webhook reception
- ✅ Multiple event handling

## API Reference Summary

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/subscriptions` | POST | Create subscription |
| `/api/subscriptions` | GET | List all subscriptions |
| `/api/subscriptions/{to}` | GET | Get specific subscription |
| `/api/subscriptions/{to}` | DELETE | Delete subscription |
| `/api/testing/create-test-event/{to}` | POST | Create simple test event |
| `/api/testing/inject-direct` | POST | Inject event directly |
| `/api/testing/inject-kafka` | POST | Inject event via Kafka |
| `/api/testing/trigger-delivery/{to}` | POST | Trigger manual delivery |
| `/api/testing/status` | GET | Get testing statistics |
| `/api/loopback/webhook` | POST | Webhook endpoint (receives events) |
| `/api/loopback/events` | GET | List received events |
| `/api/loopback/events` | DELETE | Clear received events |
| `/api/loopback/status` | GET | Get loopback status |
| `/api/monitoring/health` | GET | System health status |
| `/api/monitoring/statistics` | GET | System statistics |

## Next Steps

Once you've verified the loopback system works:

1. **Replace loopback endpoint** with your real webhook URL in subscriptions
2. **Use the event injection endpoints** to send real events to your webhook
3. **Monitor delivery** using the monitoring endpoints
4. **Scale up** by adding more subscriptions and testing throughput

The loopback system provides a complete testing environment to validate your webhook delivery pipeline before connecting to external services!