# KafkaLearning - System Architecture

## Microservices Overview

### 1. EndUser Service
**Purpose**: Acts as the Kafka Producer
**Port**: 8080
**Responsibilities**:
- Receives order requests from clients
- Publishes order messages to Kafka topics
- Manages order lifecycle

**Key Components**:
- `EndUserApplication.java`: Spring Boot entry point
- `KafkaConfigs.java`: Kafka producer configuration
- `KafkaTemplate`: For sending messages to Kafka topics
- `OrderController`: REST endpoints for order operations
- `OrderService`: Business logic for order processing

### 2. DeliveryBoyApp Service
**Purpose**: Acts as the Kafka Consumer
**Port**: 8081
**Responsibilities**:
- Consumes order messages from Kafka
- Processes delivery updates
- Sends delivery notifications

**Key Components**:
- `DeliveryBoyApplication.java`: Spring Boot entry point
- `KafkaConfig.java`: Kafka consumer configuration with topic creation
- `KafkaService.java`: Kafka message listener
- `DeliveryController`: REST endpoints for delivery operations
- `LocationService`: Manages delivery location updates

## Data Flow

```
Client Request
    |
    v
EndUser Service (8080)
    |
    | HTTP POST /api/orders
    v
OrderService (publish to Kafka)
    |
    | Kafka Topic: order-placed-topic
    v
Kafka Broker (Docker)
    |
    v
DeliveryBoyApp Service (8081)
    |
    v
KafkaService (listener)
    |
    v
LocationService (process delivery)
    |
    | Publish to delivery-updates-topic
    v
Kafka Broker
```

## Kafka Topics Configuration

### order-placed-topic
- **Partitions**: 3
- **Replication Factor**: 3
- **Message Format**: JSON (String, String)
- **Message Content**:
  - orderId
  - userId
  - items
  - totalPrice
  - timestamp

### delivery-updates-topic
- **Partitions**: 3 (planned)
- **Message Format**: JSON
- **Message Content**:
  - orderId
  - location
  - deliveryBoyId
  - status
  - timestamp

## Technology Stack

### Backend
- **Framework**: Spring Boot 3.x
- **Language**: Java 17+
- **Message Broker**: Apache Kafka 3.x
- **Container**: Docker & Docker Compose
- **Build Tool**: Maven
- **Zookeeper**: For Kafka coordination

### Infrastructure
- **Kafka**: 3 brokers (configured in docker-compose.yml)
- **Zookeeper**: Cluster management
- **Network**: Docker bridge network

## Deployment Architecture

```
Docker Compose
├── Kafka Broker 1
├── Kafka Broker 2
├── Kafka Broker 3
├── Zookeeper
├── EndUser Service
└── DeliveryBoyApp Service
```

## Configuration Management

### EndUser Service (application.yml)
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
```

### DeliveryBoyApp Service (application.yml)
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      bootstrap-servers: localhost:9092
      group-id: delivery-group
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
```

## Error Handling & Resilience

### Kafka Serialization Errors
- Handled with try-catch blocks
- Failed messages logged for debugging
- Consumer continues processing next message

### Message Processing Failures
- Logged with ERROR level
- Dead Letter Queue (DLQ) support (future enhancement)
- Retry mechanism (future enhancement)

## Monitoring & Observability

### Logging
- SLF4J with Logback
- Log levels: DEBUG, INFO, WARN, ERROR
- Structured logging in JSON format (future enhancement)

### Metrics
- Spring Boot Actuator endpoints
- Kafka consumer lag monitoring
- Message processing throughput

## Security Considerations

### Current Implementation
- No authentication/authorization
- Development mode only
- All endpoints publicly accessible

### Future Enhancements
- JWT token-based authentication
- OAuth2 integration
- HTTPS/TLS for Kafka
- API key-based access control
- Rate limiting

