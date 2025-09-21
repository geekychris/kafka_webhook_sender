#!/bin/bash

# Kubernetes Deployment Script for Kafka Webhook Sender
# This script deploys the entire system to a Kubernetes cluster

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
    echo "  Kafka Webhook Sender - Kubernetes Deployment"
    echo "================================================"
    echo -e "${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl is not installed. Please install kubectl first."
        exit 1
    fi
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check if kubectl can connect to a cluster
    if ! kubectl cluster-info &> /dev/null; then
        print_error "Cannot connect to Kubernetes cluster. Please ensure kubectl is configured."
        exit 1
    fi
    
    print_status "Prerequisites check passed ✓"
}

# Build Docker images
build_images() {
    print_status "Building Docker images..."
    
    # Build main application
    print_status "Building kafka-webhook-sender image..."
    docker build -t kafka-webhook-sender:latest .
    
    # Build webhook server
    print_status "Building test-webhook image..."
    docker build -t test-webhook:latest webhook-server/
    
    print_status "Docker images built successfully ✓"
}

# Load images to cluster (for local clusters like minikube, kind)
load_images_to_cluster() {
    local cluster_type=$1
    
    case $cluster_type in
        "minikube")
            print_status "Loading images to minikube..."
            minikube image load kafka-webhook-sender:latest
            minikube image load test-webhook:latest
            ;;
        "kind")
            print_status "Loading images to kind cluster..."
            kind load docker-image kafka-webhook-sender:latest
            kind load docker-image test-webhook:latest
            ;;
        "none"|"")
            print_status "Skipping image loading (assuming images are available in cluster)"
            ;;
        *)
            print_warning "Unknown cluster type: $cluster_type. Skipping image loading."
            ;;
    esac
}

# Deploy to Kubernetes
deploy_to_kubernetes() {
    print_status "Deploying to Kubernetes cluster..."
    
    # Apply manifests in order
    kubectl apply -f k8s/01-namespace-config.yaml
    kubectl apply -f k8s/02-kafka.yaml
    kubectl apply -f k8s/03-kafka-webhook-sender.yaml
    kubectl apply -f k8s/04-test-webhook.yaml
    
    print_status "Kubernetes manifests applied ✓"
}

# Wait for deployments to be ready
wait_for_deployments() {
    print_status "Waiting for deployments to be ready..."
    
    local deployments=("zookeeper" "kafka" "kafka-webhook-sender" "test-webhook")
    
    for deployment in "${deployments[@]}"; do
        print_status "Waiting for $deployment deployment..."
        kubectl wait --for=condition=available --timeout=300s deployment/$deployment -n kafka-webhook-sender
        print_status "$deployment is ready ✓"
    done
    
    print_status "All deployments are ready ✓"
}

# Show cluster information
show_cluster_info() {
    echo -e "${BLUE}"
    echo "================================================"
    echo "  Deployment Completed Successfully!"
    echo "================================================"
    echo -e "${NC}"
    
    echo "📋 Cluster Information:"
    kubectl get nodes -o wide
    echo ""
    
    echo "🚀 Deployed Services:"
    kubectl get services -n kafka-webhook-sender -o wide
    echo ""
    
    echo "📦 Pod Status:"
    kubectl get pods -n kafka-webhook-sender -o wide
    echo ""
    
    echo "🌐 External Access:"
    local node_ip=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || echo "localhost")
    echo "  • Kafka Webhook Sender: http://$node_ip:30080"
    echo "  • Test Webhook Server:  http://$node_ip:30089"
    echo ""
    
    echo "📋 Useful Commands:"
    echo "  • View logs:               kubectl logs -f deployment/kafka-webhook-sender -n kafka-webhook-sender"
    echo "  • Port forward:            kubectl port-forward service/kafka-webhook-sender 8080:8080 -n kafka-webhook-sender"
    echo "  • Scale deployment:        kubectl scale deployment kafka-webhook-sender --replicas=3 -n kafka-webhook-sender"
    echo "  • Delete deployment:       kubectl delete namespace kafka-webhook-sender"
    echo ""
}

# Port forward services for easy access
port_forward() {
    print_status "Setting up port forwarding..."
    
    echo "Starting port forwarding in background..."
    kubectl port-forward service/kafka-webhook-sender 8080:8080 -n kafka-webhook-sender &
    local webhook_sender_pid=$!
    
    kubectl port-forward service/test-webhook 8089:8080 -n kafka-webhook-sender &
    local test_webhook_pid=$!
    
    echo "Port forwarding PIDs: webhook-sender=$webhook_sender_pid, test-webhook=$test_webhook_pid"
    echo "To stop port forwarding: kill $webhook_sender_pid $test_webhook_pid"
    echo ""
    echo "Services are now accessible at:"
    echo "  • Kafka Webhook Sender: http://localhost:8080"
    echo "  • Test Webhook Server:  http://localhost:8089"
}

# Show usage
show_usage() {
    echo "Usage: $0 [OPTIONS] [CLUSTER_TYPE]"
    echo ""
    echo "Cluster Types:"
    echo "  minikube    Deploy to minikube cluster"
    echo "  kind        Deploy to kind cluster"
    echo "  none        Deploy without loading images (default)"
    echo ""
    echo "Options:"
    echo "  --port-forward    Set up port forwarding after deployment"
    echo "  --help           Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 minikube                    # Deploy to minikube"
    echo "  $0 kind --port-forward         # Deploy to kind with port forwarding"
    echo "  $0                             # Deploy to current kubectl context"
    echo ""
}

# Main execution
main() {
    local cluster_type="none"
    local setup_port_forward=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --port-forward)
                setup_port_forward=true
                shift
                ;;
            --help)
                show_usage
                exit 0
                ;;
            minikube|kind|none)
                cluster_type=$1
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    print_header
    check_prerequisites
    build_images
    load_images_to_cluster "$cluster_type"
    deploy_to_kubernetes
    wait_for_deployments
    show_cluster_info
    
    if [ "$setup_port_forward" = true ]; then
        port_forward
    fi
    
    print_status "🎉 Deployment completed successfully!"
}

# Run main function
main "$@"