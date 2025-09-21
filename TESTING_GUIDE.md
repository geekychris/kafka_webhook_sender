# Kafka Webhook Sender - Testing Guide

This guide provides comprehensive instructions for testing the Kafka Webhook Sender using the built-in loopback testing functionality.

## Overview

The system now includes built-in testing capabilities:

1. **Loopback Webhook Controller** - Acts as a webhook endpoint that can receive events from the system itself
2. **Event Injection Controller** - Allows manual injection of events for testing
3. **Enhanced Subscription Management** - Create and manage subscriptions for testing

## Prerequisites

1. Ensure the application is running (locally or via Docker)
2. All services are healthy (Kafka, webhook service, etc.)
3. Have `curl` or a REST client available for making API calls

## Base URLs

- **Local Development**: `http://localhost:8090`
- **Docker Environment**: `http://localhost:8081`

For the examples below, we'll use `$BASE_URL` to represent your base URL.

## Step-by-Step Testing Guide

### Step 1: Verify System Health

First, check that all systems are running:

```bash
# Check application health
curl -s $BASE_URL/actuator/health | jq .

# Check loopback status
curl -s $BASE_URL/api/loopback/status | jq .

# Check testing status
curl -s $BASE_URL/api/testing/status | jq .
```

Expected responses should show `"status": "UP"` or `"active": true`.

### Step 2: Clear Previous Test Data (Optional)

Start with a clean slate:

```bash
# Clear any previous webhook events
curl -X DELETE $BASE_URL/api/loopback/events

# Clear received events
curl -s $BASE_URL/api/loopback/status | jq .
```

### Step 3: Create a Loopback Subscription

Create a subscription that points to the loopback webhook endpoint:

```bash
curl -X POST $BASE_URL/api/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "to": "test-user",
    "deliveryMethod": "WEBHOOK",
    "endpoint": "'$BASE_URL'/api/loopback/webhook",
    "headers": {
      "X-Test-Header": "loopback-test",
      "Authorization": "Bearer test-token"
    },
    "configuration": {},
    "enabled": true,
    "maxRetries": 3,
    "retryDelayMs": 1000,
    "batchSize": 1
  }'
```

Expected response:
```json
{
  "to": "test-user",
  "deliveryMethod": "WEBHOOK",
  "endpoint": "http://localhost:8081/api/loopback/webhook",
  "enabled": true,
  ...
}
```

### Step 4: Verify Subscription

Check that the subscription was created:

```bash
# List all subscriptions
curl -s $BASE_URL/api/subscriptions | jq .

# Get specific subscription
curl -s $BASE_URL/api/subscriptions/test-user | jq .
```

### Step 5: Inject a Test Event

Now inject a test event using the built-in testing endpoint:

```bash
# Quick test event (direct injection)
curl -X POST $BASE_URL/api/testing/create-test-event/test-user \
  -H "Content-Type: application/json"
```

Or create a custom event:

```bash
curl -X POST $BASE_URL/api/testing/inject-direct \
  -H "Content-Type: application/json" \
  -d '{
    "to": "test-user",
    "eventType": "user.profile.updated",
    "description": "User profile update test event",
    "data": {
      "userId": "test-user",
      "action": "profile_update",
      "changes": {
        "email": "test@example.com",
        "name": "Test User"
      },
      "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
    }
  }'
```

Expected response:
```json
{
  "status": "success",
  "message": "Event injected directly into processing pipeline",
  "eventId": "12345-67890-abcdef",
  "to": "test-user",
  "method": "direct",
  "timestamp": "2025-09-20T22:10:45.123Z"
}
```

### Step 6: Wait for Processing

The system processes events every 5 seconds by default. Wait a moment for processing:

```bash
# Check processing status
curl -s $BASE_URL/api/testing/status | jq .

# Manually trigger delivery if needed
curl -X POST $BASE_URL/api/testing/trigger-delivery/test-user
```

### Step 7: Verify Event Delivery

Check if the event was delivered to the loopback webhook:

```bash
# Check all received events
curl -s $BASE_URL/api/loopback/events | jq .

# Check loopback status
curl -s $BASE_URL/api/loopback/status | jq .
```

Expected: You should see the event in the received events with all the data you sent.

### Step 8: Test Kafka Flow

Test the full Kafka flow by injecting via Kafka:

```bash
curl -X POST $BASE_URL/api/testing/inject-kafka \
  -H "Content-Type: application/json" \
  -d '{
    "to": "test-user",
    "eventType": "order.created",
    "description": "New order created",
    "data": {
      "orderId": "ORDER-123",
      "userId": "test-user",
      "amount": 99.99,
      "currency": "USD",
      "items": [
        {"name": "Test Product", "quantity": 1, "price": 99.99}
      ]
    }
  }'
```

Wait a moment and check for delivery:

```bash
curl -s $BASE_URL/api/loopback/events | jq .
```

## Advanced Testing Scenarios

### Multiple Recipients

Create subscriptions for multiple users and test batch processing:

```bash
# Create multiple subscriptions
for user in user-a user-b user-c; do
  curl -X POST $BASE_URL/api/subscriptions \
    -H "Content-Type: application/json" \
    -d "{
      \"to\": \"$user\",
      \"deliveryMethod\": \"WEBHOOK\",
      \"endpoint\": \"$BASE_URL/api/loopback/webhook\",
      \"enabled\": true,
      \"maxRetries\": 3,
      \"batchSize\": 1
    }"
done

# Send events to multiple users
for user in user-a user-b user-c; do
  curl -X POST $BASE_URL/api/testing/create-test-event/$user
done
```

### Error Testing

Test retry logic by temporarily disabling a subscription:

```bash
# Disable subscription
curl -X PUT $BASE_URL/api/subscriptions/test-user \
  -H "Content-Type: application/json" \
  -d '{
    "enabled": false
  }'

# Inject event (should not be delivered)
curl -X POST $BASE_URL/api/testing/create-test-event/test-user

# Re-enable and trigger delivery
curl -X PUT $BASE_URL/api/subscriptions/test-user \
  -H "Content-Type: application/json" \
  -d '{
    "enabled": true
  }'

curl -X POST $BASE_URL/api/testing/trigger-delivery/test-user
```

### Monitoring and Metrics

Check system metrics:

```bash
# Application metrics
curl -s $BASE_URL/actuator/metrics | jq .

# Prometheus metrics
curl -s $BASE_URL/actuator/prometheus

# Specific metrics
curl -s $BASE_URL/actuator/metrics/loopback.webhook.received | jq .
curl -s $BASE_URL/actuator/metrics/testing.event.direct_injection | jq .
```

## API Reference

### Loopback Webhook Controller

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/loopback/webhook` | Receive webhook events |
| GET | `/api/loopback/events` | List all received events |
| GET | `/api/loopback/events/{id}` | Get specific event |
| DELETE | `/api/loopback/events` | Clear all events |
| GET | `/api/loopback/status` | Get loopback status |

### Event Injection Controller

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/testing/inject-direct` | Inject event directly |
| POST | `/api/testing/inject-kafka` | Inject event via Kafka |
| POST | `/api/testing/create-test-event/{to}` | Create simple test event |
| POST | `/api/testing/trigger-delivery/{to}` | Trigger delivery for user |
| GET | `/api/testing/status` | Get testing status |

### Subscription Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/subscriptions` | List subscriptions |
| GET | `/api/subscriptions/{to}` | Get subscription |
| POST | `/api/subscriptions` | Create subscription |
| PUT | `/api/subscriptions/{to}` | Update subscription |
| DELETE | `/api/subscriptions/{to}` | Delete subscription |

### Test Controller

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/test/ping` | Simple health check |
| GET | `/api/test/` | Root test endpoint |

## Troubleshooting

### Events Not Being Delivered

1. Check subscription exists and is enabled:
   ```bash
   curl -s $BASE_URL/api/subscriptions/test-user | jq .enabled
   ```

2. Check for pending events:
   ```bash
   curl -s $BASE_URL/api/testing/status | jq .pendingEvents
   ```

3. Manually trigger delivery:
   ```bash
   curl -X POST $BASE_URL/api/testing/trigger-delivery/test-user
   ```

4. Check application logs for errors

### Loopback Not Receiving Events

1. Verify loopback is active:
   ```bash
   curl -s $BASE_URL/api/loopback/status | jq .active
   ```

2. Check subscription endpoint URL matches loopback URL

3. Test loopback directly:
   ```bash
   curl -X POST $BASE_URL/api/loopback/webhook \
     -H "Content-Type: application/json" \
     -d '{"test": "data"}'
   ```

### Controllers Not Found (404 errors)

This indicates the Spring Boot application is not properly discovering the controllers. Check:

1. Application is running the latest build
2. Controllers are included in the JAR file
3. Spring Boot debug logs show controller registration

## Example End-to-End Test Script

Here's a complete test script that exercises the full system:

```bash
#!/bin/bash

BASE_URL="http://localhost:8081"
USER="e2e-test-user"

echo "=== Kafka Webhook Sender E2E Test ==="

# 1. Clear previous data
echo "Clearing previous test data..."
curl -s -X DELETE $BASE_URL/api/loopback/events > /dev/null
curl -s -X DELETE $BASE_URL/api/subscriptions/$USER > /dev/null

# 2. Create loopback subscription
echo "Creating loopback subscription..."
curl -X POST $BASE_URL/api/subscriptions \
  -H "Content-Type: application/json" \
  -d "{
    \"to\": \"$USER\",
    \"deliveryMethod\": \"WEBHOOK\",
    \"endpoint\": \"$BASE_URL/api/loopback/webhook\",
    \"enabled\": true,
    \"maxRetries\": 3,
    \"batchSize\": 1
  }" > /dev/null

# 3. Inject test event
echo "Injecting test event..."
RESPONSE=$(curl -s -X POST $BASE_URL/api/testing/create-test-event/$USER)
EVENT_ID=$(echo $RESPONSE | jq -r .eventId)
echo "Event ID: $EVENT_ID"

# 4. Wait for processing
echo "Waiting for event processing..."
sleep 6

# 5. Check delivery
echo "Checking event delivery..."
RECEIVED=$(curl -s $BASE_URL/api/loopback/events)
COUNT=$(echo $RECEIVED | jq 'length')

if [ "$COUNT" -gt 0 ]; then
    echo "✅ SUCCESS: $COUNT events received"
    echo "Latest event:"
    echo $RECEIVED | jq 'to_entries | .[0].value | {eventId, receivedAt}'
else
    echo "❌ FAILURE: No events received"
    
    # Troubleshooting
    echo "Subscription status:"
    curl -s $BASE_URL/api/subscriptions/$USER | jq '{to, enabled, endpoint}'
    
    echo "Testing status:"
    curl -s $BASE_URL/api/testing/status | jq '{totalEvents, processedEvents, pendingEvents}'
    
    exit 1
fi

echo "=== Test Complete ==="
```

Save this as `test-e2e.sh` and run with `bash test-e2e.sh`.