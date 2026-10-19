<p align="center">
  <img src="assets/logo.png" alt="Kafka Webhook Sender logo" width="180">
</p>

# Kafka Webhook Sender

A production-ready Spring Boot service that consumes CloudEvents from Kafka and delivers them to webhook endpoints with ordered delivery guarantees, retry logic, and comprehensive monitoring.

## 🚀 Features

- **CloudEvent Processing**: Consumes CloudEvents from Kafka topics with configurable "to" field extraction
- **Virtual Queues**: Creates separate ordered queues based on the "to" field in events
- **Webhook Delivery**: Supports webhook delivery with CloudEvent JSON batch format
- **Ordered Delivery**: Maintains strict order of events within each virtual queue
- **Retry Logic**: Exponential backoff with jitter and configurable retry limits
- **Monitoring**: Micrometer metrics, Prometheus integration, and REST endpoints for monitoring
- **Persistent Storage**: Uses RocksDB for reliable event and state storage with atomic operations
- **Extensible Delivery**: Pluggable framework supports multiple delivery methods (Webhook, Email, SMS, etc.)
- **High Availability**: Designed for horizontal scaling and fault tolerance
- **Production Ready**: Docker, Kubernetes, monitoring, and operational tooling included

## 🏗️ Architecture

### System Overview
```
┌─────────────────┐    ┌──────────────────────┐    ┌─────────────────────┐
│   Kafka Topic   │───▶│  CloudEvent Consumer │───▶│   RocksDB Storage   │
│    (events)     │    │   (Spring Kafka)     │    │ (Ordered by seq ID) │
└─────────────────┘    └──────────────────────┘    └─────────────────────┘
                                                              │
                                                              ▼
┌─────────────────┐    ┌──────────────────────┐    ┌─────────────────────┐
│ External        │◀───│  Delivery Services   │◀───│   Virtual Queues    │
│ Endpoints       │    │  (Webhook, Email)    │    │   (per "to" field)  │
│ (Webhooks, etc.)│    │                      │    │                     │
└─────────────────┘    └──────────────────────┘    └─────────────────────┘
```

### Key Components

1. **CloudEvent Consumer**: Listens to Kafka topics and processes incoming CloudEvents
2. **Virtual Queue Manager**: Creates separate queues per "to" field to ensure ordered delivery
3. **RocksDB Storage**: Provides persistent, ordered storage for events and delivery state
4. **Delivery Services**: Extensible framework for different delivery methods (webhooks, email, SMS)
5. **Retry Engine**: Handles failed deliveries with exponential backoff and tracking
6. **Monitoring Stack**: Metrics collection, health checks, and operational visibility

### Data Flow

1. **Event Ingestion**: CloudEvents arrive via Kafka
2. **"To" Field Extraction**: Events are categorized by recipient ("to" field)
3. **Ordered Storage**: Events stored in RocksDB with sequential IDs per recipient
4. **Delivery Processing**: Scheduled service processes virtual queues in order
5. **Webhook Delivery**: Events delivered via HTTP POST in CloudEvent format
6. **State Tracking**: Delivery progress and failures tracked per recipient
7. **Retry Logic**: Failed deliveries retried with exponential backoff

## 📋 Prerequisites

### For Docker Deployment (Recommended)
- Docker 20.0+
- Docker Compose 2.0+
- curl (for testing)
- jq (optional, for formatted JSON output)

### For Local Development
- Java 21 (OpenJDK or Amazon Corretto 23 as per your JAVA_HOME)
- Apache Kafka 2.8+
- Maven 3.8+
- Node.js 18+ (for webhook server)

### For Kubernetes Deployment
- kubectl configured with cluster access
- Docker for building images
- Minikube/Kind/K8s cluster

## 🚀 Quick Start

### Option 1: Docker Compose (Recommended)

**One-command startup:**
```bash
./scripts/start.sh
```

This script will:
- Check prerequisites and build the application
- Start Kafka, Zookeeper, and all services
- Set up monitoring (Prometheus, Grafana)
- Wait for all services to be healthy
- Display access URLs and next steps

**Services will be available at:**
- 🎯 **Kafka Webhook Sender**: http://localhost:8080
- 🎣 **Test Webhook Server**: http://localhost:8089  
- 📊 **Kafka UI**: http://localhost:8090
- 📈 **Prometheus**: http://localhost:9090
- 📊 **Grafana**: http://localhost:3000 (admin/admin)

**Run the demo:**
```bash
./scripts/send-test-event.sh
```

**Stop everything:**
```bash
./scripts/stop.sh
```

### Option 2: Kubernetes Deployment

**Deploy to local cluster (Minikube/Kind):**
```bash
./scripts/deploy-k8s.sh minikube --port-forward
```

**Deploy to any Kubernetes cluster:**
```bash
./scripts/deploy-k8s.sh
```

### Option 3: Local Development

1. **Start Kafka locally:**
   ```bash
   # Download and start Kafka
   bin/kafka-server-start.sh config/server.properties
   ```

2. **Build and run:**
   ```bash
   mvn clean package
   java -jar target/kafka-webhook-sender-0.0.1-SNAPSHOT.jar
   ```

## API Endpoints

### Subscription Management

#### Create Subscription
```bash
POST /api/subscriptions
Content-Type: application/json

{
  "to": "user123",
  "deliveryMethod": "WEBHOOK",
  "endpoint": "https://your-webhook.com/endpoint",
  "headers": {
    "Authorization": "Bearer your-token"
  },
  "enabled": true,
  "maxRetries": 3,
  "retryDelayMs": 1000,
  "batchSize": 10
}
```

#### Get All Subscriptions
```bash
GET /api/subscriptions
```

#### Get Subscription by To
```bash
GET /api/subscriptions/{to}
```

#### Update Subscription
```bash
PUT /api/subscriptions/{to}
Content-Type: application/json

{
  "endpoint": "https://new-webhook.com/endpoint",
  "enabled": false
}
```

#### Delete Subscription
```bash
DELETE /api/subscriptions/{to}
```

#### Trigger Manual Delivery
```bash
POST /api/subscriptions/{to}/trigger-delivery
```

#### Get Delivery Statistics
```bash
GET /api/subscriptions/{to}/statistics
```

### Monitoring

#### System Health
```bash
GET /api/monitoring/health
```

#### System Statistics
```bash
GET /api/monitoring/statistics
```

#### Queue Status
```bash
GET /api/monitoring/queue-status
```

#### Failed Deliveries
```bash
GET /api/monitoring/failed-deliveries
```

#### Prometheus Metrics
```bash
GET /actuator/prometheus
```

## Configuration

Key configuration properties in `application.yml`:

```yaml
# Kafka Configuration
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: webhook-sender-group

# RocksDB Configuration
rocksdb:
  path: ./data/rocksdb

# Delivery Configuration
event:
  delivery:
    scheduler:
      delay: 5000  # Delivery check interval in ms
```

## CloudEvent Format

The service expects CloudEvents in JSON format. The "to" field can be provided in:

1. **Extension attribute** (preferred):
```json
{
  "specversion": "1.0",
  "type": "user.notification",
  "source": "notification-service",
  "id": "unique-id",
  "time": "2023-12-01T12:00:00Z",
  "to": "user123",
  "data": {
    "message": "Hello World"
  }
}
```

2. **Subject field** (fallback):
```json
{
  "specversion": "1.0",
  "type": "user.notification",
  "source": "notification-service",
  "id": "unique-id",
  "subject": "user123",
  "data": {
    "message": "Hello World"
  }
}
```

3. **Data field** (fallback):
```json
{
  "specversion": "1.0",
  "type": "user.notification",
  "source": "notification-service",
  "id": "unique-id",
  "data": {
    "to": "user123",
    "message": "Hello World"
  }
}
```

## Webhook Delivery Format

Events are delivered to webhooks in CloudEvent JSON format:

**Single Event:**
```json
{
  "specversion": "1.0",
  "type": "user.notification",
  "source": "notification-service",
  "id": "unique-id",
  "time": "2023-12-01T12:00:00Z",
  "to": "user123",
  "data": {
    "message": "Hello World"
  }
}
```

**Batch Events:**
```json
[
  {
    "specversion": "1.0",
    "type": "user.notification",
    "source": "notification-service",
    "id": "unique-id-1",
    "to": "user123",
    "data": { "message": "Hello World 1" }
  },
  {
    "specversion": "1.0",
    "type": "user.notification",
    "source": "notification-service",
    "id": "unique-id-2",
    "to": "user123",
    "data": { "message": "Hello World 2" }
  }
]
```

## Testing

### Send Test Events

You can send test events to Kafka using the Kafka console producer:

```bash
# Send a test CloudEvent
kafka-console-producer.sh --topic events --bootstrap-server localhost:9092

# Paste this CloudEvent JSON:
{
  "specversion": "1.0",
  "type": "test.event",
  "source": "test",
  "id": "test-1",
  "time": "2023-12-01T12:00:00Z",
  "to": "user123",
  "data": {
    "message": "Test message"
  }
}
```

### Run Tests

```bash
# Run all tests
mvn test

# Run integration tests only
mvn test -Dtest=*IntegrationTest
```

## Metrics and Monitoring

The service exposes various metrics via Micrometer:

- `kafka.events.received` - Events received from Kafka
- `kafka.events.stored` - Events successfully stored
- `event.delivery.attempts` - Delivery attempts
- `event.delivery.success` - Successful deliveries
- `event.delivery.failure` - Failed deliveries
- `webhook.delivery` - Webhook-specific delivery metrics

View metrics at: http://localhost:8080/actuator/prometheus

## Development

### Project Structure

```
src/main/java/com/example/kafkawebhooksender/
├── config/              # Configuration classes
├── controller/          # REST controllers
├── model/              # Domain models
├── repository/         # Data access layer
└── service/            # Business logic
    └── delivery/       # Delivery service implementations
```

### Adding New Delivery Methods

1. Create a new delivery service implementing `DeliveryService`
2. Add the new delivery method to `DeliveryMethod` enum
3. Register as a Spring component
4. The service will automatically be picked up by `DeliveryServiceRegistry`

Example:
```java
@Service
public class EmailDeliveryService implements DeliveryService {
    
    @Override
    public boolean supports(DeliveryMethod deliveryMethod) {
        return DeliveryMethod.EMAIL == deliveryMethod;
    }
    
    @Override
    public DeliveryResult deliver(Subscription subscription, List<StoredEvent> events) {
        // Implementation
    }
}
```

## Troubleshooting

### Common Issues

1. **Kafka Connection Issues**
   - Ensure Kafka is running on localhost:9092
   - Check network connectivity
   - Verify topic exists

2. **RocksDB Permission Issues**
   - Ensure write permissions to `./data/rocksdb` directory
   - Check disk space

3. **Webhook Delivery Failures**
   - Check webhook endpoint availability
   - Verify SSL certificates for HTTPS endpoints
   - Check firewall/network policies

### Logs

Enable debug logging for troubleshooting:

```yaml
logging:
  level:
    com.example.kafkawebhooksender: DEBUG
```

## 🚀 Operations Guide

### Docker Compose Operations

#### Starting the System
```bash
# Full system startup with health checks
./scripts/start.sh

# Start in background and follow logs
docker-compose up -d
docker-compose logs -f kafka-webhook-sender
```

#### Monitoring and Troubleshooting
```bash
# View service logs
docker logs kafka-webhook-sender
docker logs test-webhook
docker logs kafka

# Monitor system health
curl http://localhost:8080/actuator/health
curl http://localhost:8089/health

# View metrics
curl http://localhost:8080/actuator/prometheus
curl http://localhost:8089/stats
```

#### Testing and Demo
```bash
# Run complete demo
./scripts/send-test-event.sh demo

# Test failure scenarios
./scripts/send-test-event.sh failures

# Send single event
./scripts/send-test-event.sh event user123 "Hello World"

# View statistics
./scripts/send-test-event.sh stats
```

#### Scaling and Configuration
```bash
# Scale the webhook sender
docker-compose up -d --scale kafka-webhook-sender=3

# Update configuration (edit docker-compose.yml)
docker-compose up -d kafka-webhook-sender
```

#### Cleanup
```bash
# Stop services
./scripts/stop.sh

# Stop and remove data
./scripts/stop.sh --remove-data

# Complete cleanup
./scripts/stop.sh --cleanup
```

### Kubernetes Operations

#### Deployment
```bash
# Deploy to Minikube
./scripts/deploy-k8s.sh minikube

# Deploy to Kind
./scripts/deploy-k8s.sh kind --port-forward

# Deploy to any cluster
./scripts/deploy-k8s.sh
```

#### Monitoring
```bash
# Check pod status
kubectl get pods -n kafka-webhook-sender

# View logs
kubectl logs -f deployment/kafka-webhook-sender -n kafka-webhook-sender

# Port forward for local access
kubectl port-forward service/kafka-webhook-sender 8080:8080 -n kafka-webhook-sender
```

#### Scaling
```bash
# Scale deployment
kubectl scale deployment kafka-webhook-sender --replicas=3 -n kafka-webhook-sender

# Update configuration
kubectl edit configmap kafka-webhook-sender-config -n kafka-webhook-sender
kubectl rollout restart deployment/kafka-webhook-sender -n kafka-webhook-sender
```

#### Cleanup
```bash
# Remove all resources
kubectl delete namespace kafka-webhook-sender
```

### Production Deployment Considerations

#### High Availability
- Deploy multiple replicas of the webhook sender
- Use persistent volumes for RocksDB data
- Configure resource limits and requests
- Set up proper monitoring and alerting

#### Security
- Use TLS for all external communications
- Implement proper authentication for webhook endpoints
- Secure Kafka with SASL/SSL
- Use Kubernetes secrets for sensitive configuration

#### Performance Tuning
- Adjust Kafka consumer configuration based on throughput
- Tune RocksDB settings for your workload
- Configure appropriate batch sizes for webhook delivery
- Monitor and adjust JVM memory settings

#### Monitoring and Alerting
- Set up Prometheus scraping for metrics
- Configure Grafana dashboards for visualization
- Create alerts for delivery failures and high latency
- Monitor resource usage and scaling triggers

## 📊 Monitoring and Metrics

### Built-in Dashboards
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Kafka UI**: http://localhost:8090

### Key Metrics
- `kafka_events_received_total` - Events received from Kafka
- `kafka_events_stored_total` - Events successfully stored
- `event_delivery_attempts_total` - Delivery attempts
- `event_delivery_success_total` - Successful deliveries
- `event_delivery_failure_total` - Failed deliveries
- `webhook_delivery_time_seconds` - Webhook delivery latency

### Health Checks
- **Application Health**: `GET /actuator/health`
- **Webhook Server Health**: `GET /health`
- **System Statistics**: `GET /api/monitoring/statistics`
- **Queue Status**: `GET /api/monitoring/queue-status`

## 🛠️ Development

### Project Structure
```
kafka-webhook-sender/
├── src/main/java/              # Main application code
├── src/test/java/              # Test suites
├── webhook-server/             # Test webhook server (Node.js)
├── scripts/                    # Operational scripts
├── k8s/                       # Kubernetes manifests
├── monitoring/                # Monitoring configuration
└── docker-compose.yml         # Docker Compose setup
```

### Local Development Setup
```bash
# Install dependencies
mvn clean install

# Start development services
docker-compose up -d kafka zookeeper

# Run application
mvn spring-boot:run

# Run tests
mvn test
```

### Adding New Delivery Methods

1. Create delivery service:
```java
@Service
public class EmailDeliveryService implements DeliveryService {
    @Override
    public boolean supports(DeliveryMethod deliveryMethod) {
        return DeliveryMethod.EMAIL == deliveryMethod;
    }
    
    @Override
    public DeliveryResult deliver(Subscription subscription, List<StoredEvent> events) {
        // Implementation
    }
}
```

2. Add to DeliveryMethod enum
3. Service will be auto-discovered by Spring

### Testing
```bash
# Unit tests
mvn test

# Integration tests
mvn test -Dtest=*IntegrationTest

# End-to-end tests
./scripts/send-test-event.sh demo
```

## 🔧 Configuration Reference

### Application Properties
```yaml
# Kafka Configuration
spring.kafka.bootstrap-servers: localhost:9092
kafka.consumer.group-id: webhook-sender-group
kafka.topic.events: events

# Storage Configuration
rocksdb.path: ./data/rocksdb

# Delivery Configuration
event.delivery.scheduler.delay: 5000

# Monitoring
management.endpoints.web.exposure.include: health,metrics,prometheus
```

### Environment Variables
- `SPRING_KAFKA_BOOTSTRAP_SERVERS` - Kafka brokers
- `ROCKSDB_PATH` - RocksDB data directory
- `LOGGING_LEVEL_COM_EXAMPLE_KAFKAWEBHOOKSENDER` - Log level

## 🆘 Troubleshooting

### Common Issues

**Kafka Connection Issues**
```bash
# Check Kafka is running
docker logs kafka

# Test Kafka connectivity
kafka-console-producer --bootstrap-server localhost:9092 --topic events
```

**RocksDB Permission Issues**
```bash
# Check permissions
ls -la data/rocksdb/

# Fix permissions
sudo chown -R $(whoami) data/
```

**Webhook Delivery Failures**
```bash
# Check webhook server
curl http://localhost:8089/health

# View delivery statistics
curl http://localhost:8080/api/monitoring/failed-deliveries
```

**Memory Issues**
```bash
# Increase JVM memory
export JAVA_OPTS="-Xmx2g -Xms1g"

# Or in Docker Compose:
environment:
  - JAVA_OPTS=-Xmx2g -Xms1g
```

### Debug Mode
```bash
# Enable debug logging
export LOGGING_LEVEL_COM_EXAMPLE_KAFKAWEBHOOKSENDER=DEBUG

# View detailed logs
docker logs -f kafka-webhook-sender
```

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📞 Support

For issues and questions:
- Create an issue in the GitHub repository
- Check the troubleshooting guide above
- Review the operational logs
