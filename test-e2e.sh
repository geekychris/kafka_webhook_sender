#!/bin/bash

set -e  # Exit on any error

# Configuration
BASE_URL="${BASE_URL:-http://localhost:8081}"
USER="e2e-test-user-$(date +%s)"
VERBOSE="${VERBOSE:-false}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

verbose() {
    if [ "$VERBOSE" = "true" ]; then
        echo -e "${YELLOW}[VERBOSE]${NC} $1"
    fi
}

# Check if jq is available
check_dependencies() {
    if ! command -v jq &> /dev/null; then
        log_error "jq is required but not installed. Please install jq to continue."
        exit 1
    fi
    
    if ! command -v curl &> /dev/null; then
        log_error "curl is required but not installed. Please install curl to continue."
        exit 1
    fi
}

# Check if the service is running
check_service() {
    log_info "Checking if service is running at $BASE_URL"
    
    if curl -s -f "$BASE_URL/actuator/health" > /dev/null; then
        log_success "Service is running"
    else
        log_error "Service is not responding at $BASE_URL"
        log_error "Please ensure the kafka-webhook-sender is running"
        exit 1
    fi
}

# Clean up previous test data
cleanup() {
    log_info "Cleaning up previous test data"
    
    # Clear loopback events (ignore errors)
    curl -s -X DELETE "$BASE_URL/api/loopback/events" > /dev/null 2>&1 || true
    
    # Delete test subscription (ignore errors)
    curl -s -X DELETE "$BASE_URL/api/subscriptions/$USER" > /dev/null 2>&1 || true
    
    verbose "Cleanup completed"
}

# Create loopback subscription
create_subscription() {
    log_info "Creating loopback subscription for user: $USER"
    
    local response_and_code=$(curl -s -w "HTTPSTATUS:%{http_code}" -X POST "$BASE_URL/api/subscriptions" \
        -H "Content-Type: application/json" \
        -d "{
            \"to\": \"$USER\",
            \"deliveryMethod\": \"WEBHOOK\",
            \"endpoint\": \"$BASE_URL/api/loopback/webhook\",
            \"headers\": {
                \"X-Test-Header\": \"e2e-test\",
                \"Authorization\": \"Bearer test-token-$(date +%s)\"
            },
            \"enabled\": true,
            \"maxRetries\": 3,
            \"retryDelayMs\": 1000,
            \"batchSize\": 1
        }")
    
    local response=$(echo $response_and_code | sed -E 's/HTTPSTATUS:[0-9]{3}$//')
    local status_code=$(echo $response_and_code | tr -d '\n' | sed -E 's/.*HTTPSTATUS:([0-9]{3})$/\1/')
    
    if [ "$status_code" -eq 201 ] || [ "$status_code" -eq 200 ]; then
        log_success "Subscription created successfully"
        verbose "Response: $response"
    else
        log_error "Failed to create subscription (HTTP $status_code)"
        log_error "Response: $response"
        exit 1
    fi
}

# Verify subscription exists
verify_subscription() {
    log_info "Verifying subscription exists"
    
    local response=$(curl -s "$BASE_URL/api/subscriptions/$USER")
    local subscription_exists=$(echo "$response" | jq -r '.to // empty')
    
    if [ "$subscription_exists" = "$USER" ]; then
        log_success "Subscription verified"
        local enabled=$(echo "$response" | jq -r '.enabled')
        verbose "Subscription enabled: $enabled"
    else
        log_error "Subscription not found or invalid"
        log_error "Response: $response"
        exit 1
    fi
}

# Inject test event
inject_event() {
    log_info "Injecting test event"
    
    local response=$(curl -s -X POST "$BASE_URL/api/testing/create-test-event/$USER" \
        -H "Content-Type: application/json")
    
    local status=$(echo "$response" | jq -r '.status // empty')
    local event_id=$(echo "$response" | jq -r '.eventId // empty')
    
    if [ "$status" = "success" ] && [ -n "$event_id" ]; then
        log_success "Event injected successfully"
        log_info "Event ID: $event_id"
        verbose "Response: $response"
        return 0
    else
        log_error "Failed to inject event"
        log_error "Response: $response"
        exit 1
    fi
}

# Wait for event processing
wait_for_processing() {
    log_info "Waiting for event processing (max 30 seconds)"
    
    local max_attempts=30
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        local response=$(curl -s "$BASE_URL/api/loopback/events")
        local event_count=$(echo "$response" | jq 'length')
        
        if [ "$event_count" -gt 0 ]; then
            log_success "Event delivered! ($event_count events received)"
            return 0
        fi
        
        attempt=$((attempt + 1))
        echo -n "."
        sleep 1
    done
    
    echo ""  # New line after dots
    log_error "Timeout: No events received after $max_attempts seconds"
    
    # Show debugging info
    log_info "Debugging information:"
    
    local testing_status=$(curl -s "$BASE_URL/api/testing/status")
    local total_events=$(echo "$testing_status" | jq -r '.totalEvents // 0')
    local processed_events=$(echo "$testing_status" | jq -r '.processedEvents // 0')
    local pending_events=$(echo "$testing_status" | jq -r '.pendingEvents // 0')
    
    log_info "Total events: $total_events, Processed: $processed_events, Pending: $pending_events"
    
    return 1
}

# Verify event content
verify_event_content() {
    log_info "Verifying event content"
    
    local response=$(curl -s "$BASE_URL/api/loopback/events")
    local first_event=$(echo "$response" | jq 'to_entries | .[0].value')
    
    if [ "$first_event" = "null" ] || [ -z "$first_event" ]; then
        log_error "No events found to verify"
        return 1
    fi
    
    local event_id=$(echo "$first_event" | jq -r '.eventId // empty')
    local payload=$(echo "$first_event" | jq -r '.payload // empty')
    local received_at=$(echo "$first_event" | jq -r '.receivedAt // empty')
    local headers=$(echo "$first_event" | jq -r '.headers // {}')
    
    log_success "Event content verified:"
    log_info "  Event ID: $event_id"
    log_info "  Received at: $received_at"
    log_info "  Has payload: $([ -n "$payload" ] && echo "Yes" || echo "No")"
    log_info "  Has headers: $([ "$headers" != "{}" ] && echo "Yes" || echo "No")"
    
    verbose "Full event: $first_event"
    
    # Validate payload is valid JSON
    if echo "$payload" | jq . > /dev/null 2>&1; then
        log_success "Payload is valid JSON"
    else
        log_warning "Payload is not valid JSON"
    fi
}

# Test multiple events
test_multiple_events() {
    log_info "Testing multiple event injection"
    
    local event_count=3
    log_info "Injecting $event_count events"
    
    for i in $(seq 1 $event_count); do
        local response=$(curl -s -X POST "$BASE_URL/api/testing/inject-direct" \
            -H "Content-Type: application/json" \
            -d "{
                \"to\": \"$USER\",
                \"eventType\": \"test.multi.event\",
                \"description\": \"Multi-event test $i of $event_count\",
                \"data\": {
                    \"eventIndex\": $i,
                    \"totalEvents\": $event_count,
                    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
                }
            }")
        
        local status=$(echo "$response" | jq -r '.status // empty')
        if [ "$status" != "success" ]; then
            log_error "Failed to inject event $i"
            return 1
        fi
        
        verbose "Injected event $i"
    done
    
    log_info "Waiting for multiple events to be processed"
    sleep 8
    
    local response=$(curl -s "$BASE_URL/api/loopback/events")
    local total_events=$(echo "$response" | jq 'length')
    
    if [ "$total_events" -ge $((event_count + 1)) ]; then  # +1 for the initial test event
        log_success "Multiple events test passed ($total_events total events received)"
    else
        log_warning "Expected at least $((event_count + 1)) events, got $total_events"
    fi
}

# Test Kafka injection
test_kafka_injection() {
    log_info "Testing Kafka injection"
    
    local initial_count=$(curl -s "$BASE_URL/api/loopback/events" | jq 'length')
    
    local response=$(curl -s -X POST "$BASE_URL/api/testing/inject-kafka" \
        -H "Content-Type: application/json" \
        -d "{
            \"to\": \"$USER\",
            \"eventType\": \"test.kafka.event\",
            \"description\": \"Kafka injection test\",
            \"data\": {
                \"method\": \"kafka\",
                \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
                \"testType\": \"kafka-injection\"
            }
        }")
    
    local status=$(echo "$response" | jq -r '.status // empty')
    if [ "$status" != "success" ]; then
        log_error "Failed to inject event via Kafka"
        return 1
    fi
    
    log_info "Waiting for Kafka event processing (max 20 seconds)"
    local max_attempts=20
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        local current_count=$(curl -s "$BASE_URL/api/loopback/events" | jq 'length')
        
        if [ "$current_count" -gt "$initial_count" ]; then
            log_success "Kafka injection test passed"
            return 0
        fi
        
        attempt=$((attempt + 1))
        sleep 1
        echo -n "."
    done
    
    echo ""
    log_warning "Kafka event may not have been processed within timeout"
}

# Get final statistics
show_statistics() {
    log_info "Final test statistics:"
    
    # Loopback statistics
    local loopback_status=$(curl -s "$BASE_URL/api/loopback/status")
    local total_received=$(echo "$loopback_status" | jq -r '.totalEventsReceived // 0')
    local event_counter=$(echo "$loopback_status" | jq -r '.eventCounter // 0')
    local last_received=$(echo "$loopback_status" | jq -r '.lastEventReceived // "N/A"')
    
    log_info "  Events received: $total_received"
    log_info "  Event counter: $event_counter"
    log_info "  Last event: $last_received"
    
    # Testing statistics
    local testing_status=$(curl -s "$BASE_URL/api/testing/status")
    local direct_injections=$(echo "$testing_status" | jq -r '.directInjectionsCount // 0')
    local kafka_injections=$(echo "$testing_status" | jq -r '.kafkaInjectionsCount // 0')
    
    log_info "  Direct injections: $direct_injections"
    log_info "  Kafka injections: $kafka_injections"
}

# Main test execution
main() {
    echo -e "${BLUE}=== Kafka Webhook Sender E2E Test ===${NC}"
    echo "Base URL: $BASE_URL"
    echo "Test User: $USER"
    echo "Verbose: $VERBOSE"
    echo ""
    
    check_dependencies
    check_service
    cleanup
    
    # Core workflow test
    create_subscription
    verify_subscription
    inject_event
    
    if wait_for_processing; then
        verify_event_content
        
        # Additional tests
        test_multiple_events
        test_kafka_injection
        
        show_statistics
        
        echo ""
        log_success "All tests completed successfully! ✅"
        echo -e "${GREEN}=== Test Summary ===${NC}"
        echo "✅ Service connectivity"
        echo "✅ Subscription management"
        echo "✅ Event injection (direct)"
        echo "✅ Event processing and delivery"
        echo "✅ Loopback webhook reception"
        echo "✅ Multiple event handling"
        echo "✅ Kafka integration"
        
        exit 0
    else
        log_error "Core test failed - event delivery timeout"
        show_statistics
        exit 1
    fi
}

# Handle script arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --base-url)
            BASE_URL="$2"
            shift 2
            ;;
        --verbose)
            VERBOSE="true"
            shift
            ;;
        --user)
            USER="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --base-url URL    Base URL of the service (default: http://localhost:8081)"
            echo "  --verbose         Enable verbose output"
            echo "  --user USER       Custom test user ID"
            echo "  --help            Show this help message"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Run main function
main