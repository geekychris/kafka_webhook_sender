#!/bin/bash

# Test Event Sender Script
# This script demonstrates how to send test events and set up subscriptions

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Service endpoints
WEBHOOK_SENDER_URL="http://localhost:8090"
TEST_WEBHOOK_URL="http://localhost:8089"

# Check if services are running
check_services() {
    print_status "Checking if services are running..."
    
    if ! curl -f $WEBHOOK_SENDER_URL/actuator/health &> /dev/null; then
        print_error "Kafka Webhook Sender is not running. Please start it first with: ./scripts/start.sh"
        exit 1
    fi
    
    if ! curl -f $TEST_WEBHOOK_URL/health &> /dev/null; then
        print_error "Test webhook server is not running. Please start it first with: ./scripts/start.sh"
        exit 1
    fi
    
    print_status "All services are running ✓"
}

# Create a test subscription
create_subscription() {
    local user_id=$1
    local endpoint=$2
    
    print_status "Creating subscription for user: $user_id"
    
    curl -X POST $WEBHOOK_SENDER_URL/api/subscriptions \
        -H "Content-Type: application/json" \
        -d "{
            \"to\": \"$user_id\",
            \"deliveryMethod\": \"WEBHOOK\",
            \"endpoint\": \"$endpoint\",
            \"headers\": {
                \"Authorization\": \"Bearer test-token-123\",
                \"X-Custom-Header\": \"test-value\"
            },
            \"enabled\": true,
            \"maxRetries\": 3,
            \"retryDelayMs\": 1000,
            \"batchSize\": 5
        }" | jq '.'
    
    echo ""
    print_status "Subscription created ✓"
}

# Send CloudEvent to Kafka
send_cloudevent() {
    local user_id=$1
    local message=$2
    local event_type=$3
    local event_id=$(uuidgen 2>/dev/null || echo "$(date +%s)-$(shuf -i 1000-9999 -n 1 2>/dev/null || echo $RANDOM)")
    
    print_status "Sending CloudEvent for user: $user_id"
    
    # Create CloudEvent JSON
    local cloudevent=$(cat <<EOF
{
  "specversion": "1.0",
  "type": "$event_type",
  "source": "test-script",
  "id": "$event_id",
  "time": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "to": "$user_id",
  "datacontenttype": "application/json",
  "data": {
    "message": "$message",
    "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "metadata": {
      "source": "test-script",
      "version": "1.0"
    }
  }
}
EOF
)
    
    # Send to Kafka using Docker
    echo "$cloudevent" | docker exec -i kafka kafka-console-producer \
        --bootstrap-server localhost:29092 \
        --topic events
    
    echo ""
    print_status "CloudEvent sent ✓"
    echo "Event ID: $event_id"
    echo "User ID: $user_id"
    echo "Message: $message"
    echo ""
}

# Show webhook statistics
show_webhook_stats() {
    print_status "Webhook server statistics:"
    curl -s $TEST_WEBHOOK_URL/stats | jq '.'
    echo ""
}

# Show delivery statistics
show_delivery_stats() {
    local user_id=$1
    print_status "Delivery statistics for user: $user_id"
    curl -s $WEBHOOK_SENDER_URL/api/subscriptions/$user_id/statistics | jq '.'
    echo ""
}

# List recent webhooks
list_recent_webhooks() {
    print_status "Recent webhook deliveries (last 5):"
    curl -s "$TEST_WEBHOOK_URL/webhooks?limit=5" | jq '.webhooks[] | {timestamp, url, eventCount: (.body | if type == "array" then length else 1 end)}'
    echo ""
}

# Main demo function
run_demo() {
    print_status "🚀 Running Kafka Webhook Sender Demo"
    echo ""
    
    # Test different scenarios
    local users=("user123" "user456" "user789")
    local messages=("Welcome to the system!" "Your order has been processed" "Payment received successfully")
    local event_types=("user.welcome" "order.processed" "payment.received")
    
    for i in "${!users[@]}"; do
        local user=${users[$i]}
        local message=${messages[$i]}
        local event_type=${event_types[$i]}
        
        # Create subscription
        create_subscription "$user" "$TEST_WEBHOOK_URL/webhook"
        sleep 2
        
        # Send event
        send_cloudevent "$user" "$message" "$event_type"
        sleep 3
        
        # Show stats
        show_delivery_stats "$user"
    done
    
    # Send batch events for one user
    print_status "Sending batch events for user123..."
    for i in {1..3}; do
        send_cloudevent "user123" "Batch message $i" "batch.event"
        sleep 1
    done
    
    sleep 5
    
    # Show final statistics
    print_status "📊 Final Results:"
    echo ""
    show_webhook_stats
    list_recent_webhooks
    
    print_status "✅ Demo completed!"
    echo ""
    print_status "🔍 You can view more details at:"
    echo "  • Webhook deliveries: $TEST_WEBHOOK_URL/webhooks"
    echo "  • System monitoring: $WEBHOOK_SENDER_URL/api/monitoring/statistics"
    echo "  • Grafana dashboard: http://localhost:3000"
}

# Test failure scenarios
test_failures() {
    print_status "🧪 Testing failure scenarios..."
    echo ""
    
    # Create subscription to failing endpoint
    create_subscription "failure-test" "$TEST_WEBHOOK_URL/webhook/fail"
    sleep 2
    
    # Send event that will fail
    send_cloudevent "failure-test" "This will fail" "test.failure"
    sleep 10
    
    # Show retry statistics
    show_delivery_stats "failure-test"
    
    print_status "Check the logs to see retry attempts:"
    echo "  docker logs kafka-webhook-sender"
}

# Show usage
show_usage() {
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  demo        Run the complete demo (default)"
    echo "  failures    Test failure and retry scenarios"
    echo "  event       Send a single test event"
    echo "  stats       Show current statistics"
    echo "  help        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 demo                    # Run full demonstration"
    echo "  $0 event user123 \"Hello!\"  # Send single event"
    echo "  $0 stats                   # Show current stats"
    echo ""
}

# Main execution
main() {
    case "${1:-demo}" in
        "demo")
            check_services
            run_demo
            ;;
        "failures")
            check_services
            test_failures
            ;;
        "event")
            check_services
            if [ $# -lt 3 ]; then
                print_error "Usage: $0 event <user_id> <message>"
                exit 1
            fi
            create_subscription "$2" "$TEST_WEBHOOK_URL/webhook"
            sleep 2
            send_cloudevent "$2" "$3" "test.event"
            sleep 3
            show_delivery_stats "$2"
            ;;
        "stats")
            check_services
            show_webhook_stats
            echo "📋 System Statistics:"
            curl -s $WEBHOOK_SENDER_URL/api/monitoring/statistics | jq '.'
            ;;
        "help"|"--help"|"-h")
            show_usage
            ;;
        *)
            print_error "Unknown command: $1"
            show_usage
            exit 1
            ;;
    esac
}

# Check for required tools
if ! command -v curl &> /dev/null; then
    print_error "curl is required but not installed."
    exit 1
fi

if ! command -v jq &> /dev/null; then
    print_warning "jq is not installed. JSON output will be less formatted."
fi

# Run main function
main "$@"