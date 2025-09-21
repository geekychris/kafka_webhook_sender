#!/bin/bash

# Project Overview Script
# Displays project structure and getting started information

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}"
    echo "=============================================================="
    echo "  🚀 Kafka Webhook Sender - Production Ready Event Delivery"
    echo "=============================================================="
    echo -e "${NC}"
}

print_features() {
    echo -e "${GREEN}📋 Key Features:${NC}"
    echo "  ✅ CloudEvent Processing with Kafka Integration"
    echo "  ✅ Virtual Queues with Ordered Delivery per Recipient"
    echo "  ✅ RocksDB Persistent Storage with Atomic Operations"
    echo "  ✅ Webhook Delivery with CloudEvent JSON Batch Format"
    echo "  ✅ Exponential Backoff Retry Logic with Failure Tracking"
    echo "  ✅ Comprehensive Monitoring (Prometheus, Grafana, Micrometer)"
    echo "  ✅ REST API for Subscription Management and Statistics"
    echo "  ✅ Docker, Kubernetes, and Production Tooling"
    echo "  ✅ Extensible Delivery Framework (Webhook, Email, SMS)"
    echo "  ✅ High Availability and Horizontal Scaling Support"
    echo ""
}

print_architecture() {
    echo -e "${CYAN}🏗️ System Architecture:${NC}"
    echo ""
    echo "  ┌─────────────┐    ┌─────────────────────┐    ┌────────────────┐"
    echo "  │   Kafka     │───▶│  CloudEvent        │───▶│   RocksDB      │"
    echo "  │   Topic     │    │  Consumer           │    │   Storage      │"
    echo "  │  (events)   │    │ (Spring Kafka)      │    │ (Ordered)      │"
    echo "  └─────────────┘    └─────────────────────┘    └────────────────┘"
    echo "                                                        │"
    echo "                                                        ▼"
    echo "  ┌─────────────┐    ┌─────────────────────┐    ┌────────────────┐"
    echo "  │  External   │◀───│   Delivery          │◀───│  Virtual       │"
    echo "  │  Endpoints  │    │   Services          │    │  Queues        │"
    echo "  │ (Webhooks)  │    │ (Webhook, Email)    │    │ (per 'to')     │"
    echo "  └─────────────┘    └─────────────────────┘    └────────────────┘"
    echo ""
}

print_project_structure() {
    echo -e "${PURPLE}📁 Project Structure:${NC}"
    echo ""
    echo "  kafka-webhook-sender/"
    echo "  ├── 📋 README.md                    # Comprehensive documentation"
    echo "  ├── 🔧 pom.xml                      # Maven configuration"
    echo "  ├── 🐳 docker-compose.yml           # Full system with monitoring"
    echo "  ├── 🐳 Dockerfile                   # Application container"
    echo "  ├── 📝 src/main/java/               # Spring Boot application"
    echo "  │   ├── 🎯 controller/              # REST API endpoints"
    echo "  │   ├── 🔧 config/                  # Spring configuration"
    echo "  │   ├── 📊 model/                   # Domain objects"
    echo "  │   ├── 💾 repository/              # RocksDB data access"
    echo "  │   └── ⚙️ service/                 # Business logic"
    echo "  │       └── 📤 delivery/            # Delivery services"
    echo "  ├── 🧪 src/test/java/               # Comprehensive test suite"
    echo "  ├── 🎣 webhook-server/              # Node.js test webhook server"
    echo "  ├── 📊 monitoring/                  # Prometheus & Grafana config"
    echo "  ├── ⚙️ scripts/                     # Operational scripts"
    echo "  └── ☸️ k8s/                         # Kubernetes manifests"
    echo ""
}

print_quick_start() {
    echo -e "${YELLOW}⚡ Quick Start Options:${NC}"
    echo ""
    echo "  🐳 Docker Compose (Recommended):"
    echo "     ./scripts/start.sh              # Start full system"
    echo "     ./scripts/send-test-event.sh    # Run demo"
    echo "     ./scripts/stop.sh               # Stop everything"
    echo ""
    echo "  ☸️ Kubernetes:"
    echo "     ./scripts/deploy-k8s.sh minikube --port-forward"
    echo ""
    echo "  💻 Local Development:"
    echo "     mvn spring-boot:run             # Run application only"
    echo ""
}

print_endpoints() {
    echo -e "${GREEN}🌐 Service Endpoints (after ./scripts/start.sh):${NC}"
    echo ""
    echo "  🎯 Kafka Webhook Sender:  http://localhost:8080"
    echo "     • REST API:            /api/subscriptions"
    echo "     • Health Check:        /actuator/health"
    echo "     • Metrics:             /actuator/prometheus"
    echo "     • Monitoring:          /api/monitoring/statistics"
    echo ""
    echo "  🎣 Test Webhook Server:   http://localhost:8089"
    echo "     • Main endpoint:       /webhook"
    echo "     • Statistics:          /stats"
    echo "     • Recent webhooks:     /webhooks"
    echo ""
    echo "  📊 Monitoring Dashboards:"
    echo "     • Kafka UI:            http://localhost:8090"
    echo "     • Prometheus:          http://localhost:9090"
    echo "     • Grafana:             http://localhost:3000 (admin/admin)"
    echo ""
}

print_key_commands() {
    echo -e "${CYAN}🔧 Key Commands:${NC}"
    echo ""
    echo "  📦 System Management:"
    echo "     ./scripts/start.sh              # Start full system"
    echo "     ./scripts/stop.sh               # Stop services"
    echo "     ./scripts/stop.sh --cleanup     # Full cleanup"
    echo ""
    echo "  🧪 Testing & Demo:"
    echo "     ./scripts/send-test-event.sh demo        # Full demo"
    echo "     ./scripts/send-test-event.sh failures    # Test retries"
    echo "     ./scripts/send-test-event.sh stats       # Show statistics"
    echo ""
    echo "  📊 Monitoring:"
    echo "     docker logs kafka-webhook-sender         # View logs"
    echo "     curl localhost:8080/actuator/health      # Health check"
    echo "     curl localhost:8089/stats                # Webhook stats"
    echo ""
    echo "  ☸️ Kubernetes:"
    echo "     ./scripts/deploy-k8s.sh minikube         # Deploy to K8s"
    echo "     kubectl get pods -n kafka-webhook-sender # Check status"
    echo ""
}

print_example_usage() {
    echo -e "${PURPLE}💡 Example Usage Flow:${NC}"
    echo ""
    echo "  1️⃣  Start the system:"
    echo "      ./scripts/start.sh"
    echo ""
    echo "  2️⃣  Create a subscription:"
    echo '      curl -X POST localhost:8080/api/subscriptions \'
    echo '        -H "Content-Type: application/json" \'
    echo '        -d '"'"'{'
    echo '          "to": "user123",'
    echo '          "deliveryMethod": "WEBHOOK",'
    echo '          "endpoint": "http://localhost:8089/webhook",'
    echo '          "batchSize": 5'
    echo '        }'"'"
    echo ""
    echo "  3️⃣  Send test events:"
    echo "      ./scripts/send-test-event.sh event user123 \"Hello World\""
    echo ""
    echo "  4️⃣  Monitor delivery:"
    echo "      curl localhost:8080/api/subscriptions/user123/statistics"
    echo "      curl localhost:8089/webhooks"
    echo ""
}

print_production_notes() {
    echo -e "${YELLOW}🚀 Production Deployment Notes:${NC}"
    echo ""
    echo "  🔐 Security:"
    echo "     • Configure TLS for all external communications"
    echo "     • Use proper authentication for webhook endpoints"
    echo "     • Secure Kafka with SASL/SSL authentication"
    echo ""
    echo "  📊 Monitoring:"
    echo "     • Set up Prometheus scraping for metrics"
    echo "     • Configure alerts for delivery failures"
    echo "     • Monitor resource usage and scaling triggers"
    echo ""
    echo "  ⚡ Performance:"
    echo "     • Tune Kafka consumer settings for throughput"
    echo "     • Adjust RocksDB configuration for workload"
    echo "     • Configure appropriate JVM memory settings"
    echo ""
    echo "  🏗️ High Availability:"
    echo "     • Deploy multiple application replicas"
    echo "     • Use persistent volumes for RocksDB data"
    echo "     • Set up proper resource limits and requests"
    echo ""
}

main() {
    clear
    print_header
    print_features
    print_architecture
    print_project_structure
    print_quick_start
    print_endpoints
    print_key_commands
    print_example_usage
    print_production_notes
    
    echo -e "${GREEN}🎉 Ready to get started! Run: ${YELLOW}./scripts/start.sh${NC}"
    echo ""
}

main "$@"