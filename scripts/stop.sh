#!/bin/bash

# Kafka Webhook Sender - Stop Script
# This script stops the entire system

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

print_header() {
    echo -e "${BLUE}"
    echo "================================================"
    echo "  Kafka Webhook Sender - System Shutdown"
    echo "================================================"
    echo -e "${NC}"
}

# Stop the system
stop_system() {
    print_status "Stopping all services..."
    
    # Use docker-compose if available, otherwise use docker compose
    if command -v docker-compose &> /dev/null; then
        docker-compose down
    else
        docker compose down
    fi
    
    print_status "All services stopped ✓"
}

# Optional: Remove volumes
remove_data() {
    if [ "$1" = "--remove-data" ]; then
        print_warning "Removing all data volumes..."
        
        if command -v docker-compose &> /dev/null; then
            docker-compose down -v
        else
            docker compose down -v
        fi
        
        print_status "Data volumes removed ✓"
    fi
}

# Optional: Clean up everything
cleanup() {
    if [ "$1" = "--cleanup" ]; then
        print_warning "Performing full cleanup (removing containers, networks, volumes, and images)..."
        
        if command -v docker-compose &> /dev/null; then
            docker-compose down -v --remove-orphans --rmi all
        else
            docker compose down -v --remove-orphans --rmi all
        fi
        
        print_status "Full cleanup completed ✓"
    fi
}

# Show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --remove-data    Also remove data volumes"
    echo "  --cleanup        Full cleanup (containers, networks, volumes, images)"
    echo "  --help          Show this help message"
    echo ""
}

# Main execution
main() {
    if [ "$1" = "--help" ]; then
        show_usage
        exit 0
    fi
    
    print_header
    stop_system
    remove_data "$1"
    cleanup "$1"
    
    print_status "System shutdown completed!"
}

# Run main function
main "$@"