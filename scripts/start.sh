#!/bin/bash

# Kafka Webhook Sender - Startup Script
# This script starts the entire system using Docker Compose

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}"
    echo "================================================"
    echo "  Kafka Webhook Sender - System Startup"
    echo "================================================"
    echo -e "${NC}"
}

# Check if Docker and Docker Compose are installed
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    print_status "Prerequisites check passed ✓"
}

# Build the application
build_application() {
    print_status "Building the application..."
    
    # Create Maven wrapper if it doesn't exist
    if [ ! -f "./mvnw" ]; then
        print_warning "Maven wrapper not found. Creating it..."
        if command -v mvn &> /dev/null; then
            mvn -N io.takari:maven:wrapper
        else
            print_error "Maven is not installed and mvnw is missing. Please install Maven or add mvnw to the project."
            exit 1
        fi
    fi
    
    # Make mvnw executable
    chmod +x ./mvnw
    
    print_status "Application build completed ✓"
}

# Create necessary directories
create_directories() {
    print_status "Creating necessary directories..."
    
    mkdir -p data/rocksdb
    mkdir -p webhook-server/logs
    mkdir -p monitoring/grafana/dashboards
    mkdir -p monitoring/grafana/datasources
    
    print_status "Directories created ✓"
}

# Start the system
start_system() {
    print_status "Starting the system with Docker Compose..."
    
    # Use docker-compose if available, otherwise use docker compose
    if command -v docker-compose &> /dev/null; then
        docker-compose up -d --build
    else
        docker compose up -d --build
    fi
    
    print_status "System startup initiated ✓"
}

# Wait for services to be healthy
wait_for_services() {
    print_status "Waiting for services to become healthy..."
    
    local max_attempts=60
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        print_status "Health check attempt $attempt/$max_attempts"
        
        # Check Kafka
        if ! curl -f http://localhost:8090 &> /dev/null; then
            print_warning "Kafka UI not ready yet..."
        else
            print_status "Kafka UI is ready ✓"
        fi
        
        # Check Test Webhook
        if ! curl -f http://localhost:8089/health &> /dev/null; then
            print_warning "Test webhook server not ready yet..."
        else
            print_status "Test webhook server is ready ✓"
        fi
        
        # Check Main Application
        if ! curl -f http://localhost:8080/actuator/health &> /dev/null; then
            print_warning "Main application not ready yet..."
        else
            print_status "Main application is ready ✓"
            break
        fi
        
        if [ $attempt -eq $max_attempts ]; then
            print_error "Services failed to start within expected time"
            exit 1
        fi
        
        sleep 5
        attempt=$((attempt + 1))
    done
}

# Display system information
show_system_info() {
    echo -e "${BLUE}"
    echo "================================================"
    echo "  System Started Successfully!"
    echo "================================================"
    echo -e "${NC}"
    
    echo "🚀 Services are now running:"
    echo "  • Kafka Webhook Sender:  http://localhost:8080"
    echo "  • Test Webhook Server:   http://localhost:8089"
    echo "  • Kafka UI:             http://localhost:8090"
    echo "  • Prometheus:           http://localhost:9090"
    echo "  • Grafana:              http://localhost:3000 (admin/admin)"
    echo ""
    echo "📋 Useful endpoints:"
    echo "  • API Documentation:     http://localhost:8080/api/subscriptions"
    echo "  • Health Check:          http://localhost:8080/actuator/health"
    echo "  • Metrics:               http://localhost:8080/actuator/prometheus"
    echo "  • Webhook Stats:         http://localhost:8089/stats"
    echo ""
    echo "📁 Log files:"
    echo "  • Application logs:      docker logs kafka-webhook-sender"
    echo "  • Webhook logs:          docker logs test-webhook"
    echo ""
    echo "⚡ Quick test:"
    echo "  Run: ./scripts/send-test-event.sh"
    echo ""
}

# Main execution
main() {
    print_header
    check_prerequisites
    create_directories
    build_application
    start_system
    wait_for_services
    show_system_info
}

# Run main function
main "$@"