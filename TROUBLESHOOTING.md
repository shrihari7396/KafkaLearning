# Troubleshooting Guide

## Common Issues and Solutions

### 1. Kafka Connection Issues

#### Problem: "Connection refused" error
```
org.apache.kafka.common.errors.KafkaException: Connection refused
```

**Solution**:
- Ensure Kafka is running: `docker-compose ps`
- Check if Kafka container is healthy: `docker logs kafka`
- Restart Kafka: `docker-compose restart kafka`
- Verify bootstrap servers configuration in application.yml

#### Problem: "Timeout" when connecting to Kafka
```
Timeout expired while fetching topic metadata
```

**Solution**:
- Increase Kafka startup time: Wait 10-15 seconds after `docker-compose up`
- Check network connectivity: `docker network ls`
- Verify KAFKA_ADVERTISED_LISTENERS in docker-compose.yml

### 2. Topic-Related Issues

#### Problem: Topic "order-placed-topic" not found

**Solution**:
- Ensure DeliveryBoyApp service has started (it creates topics on startup)
- Manually create topic:
```bash
docker exec -it <kafka-container> \
  kafka-topics --create --topic order-placed-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

#### Problem: Consumer group not consuming messages

**Solution**:
- Check consumer group status:
```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group delivery-group --describe
```
- Reset consumer offset:
```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group delivery-group --reset-offsets --to-earliest --execute
```

### 3. Serialization Issuescd EndUser && cat > src/main/java/com/enduser/dtos/OrderRequest.java << 'EOF'
package com.enduser.dtos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * DTO for Order Request
 * Represents an order placed by a user
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderRequest {
    private String orderId;
    private String userId;
    private String items;
    private BigDecimal totalPrice;
    private LocalDateTime timestamp;
    private String deliveryAddress;
    private String phoneNumber;

    public OrderRequest(String orderId, String userId, String items, BigDecimal totalPrice) {
        this.orderId = orderId;
        this.userId = userId;
        this.items = items;
        this.totalPrice = totalPrice;
        this.timestamp = LocalDateTime.now();
    }
}
EOF

cat > src/main/java/com/enduser/service/OrderService.java << 'EOF'
package com.enduser.service;

import com.enduser.dtos.OrderRequest;
import com.enduser.dtos.OrderResponse;
import com.enduser.config.AppConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import java.util.UUID;

/**
 * Service for handling order operations
 * Publishes orders to Kafka topic for processing
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Place a new order and publish to Kafka
     * @param orderRequest Order details
     * @return OrderResponse with order details
     */
    public OrderResponse placeOrder(OrderRequest orderRequest) {
        try {
            log.info("Placing order for user: {}", orderRequest.getUserId());
            
            // Set generated fields
            if (orderRequest.getOrderId() == null) {
                orderRequest.setOrderId("ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
            }

            // Convert to JSON
            String orderJson = objectMapper.writeValueAsString(orderRequest);
            
            // Create Kafka message with partition key
            Message<String> message = MessageBuilder
                    .withPayload(orderJson)
                    .setHeader(KafkaHeaders.TOPIC, AppConstants.ORDER_PLACED_TOPIC)
                    .setHeader(KafkaHeaders.MESSAGE_KEY, orderRequest.getUserId())
                    .setHeader("orderId", orderRequest.getOrderId())
                    .setHeader("timestamp", System.currentTimeMillis())
                    .build();
            
            // Send to Kafka
            var sendResult = kafkaTemplate.send(message).get();
            
            log.info("Order published successfully. OrderId: {}, Topic: {}, Partition: {}, Offset: {}",
                    orderRequest.getOrderId(),
                    sendResult.getRecordMetadata().topic(),
                    sendResult.getRecordMetadata().partition(),
                    sendResult.getRecordMetadata().offset());
            
            // Return success response
            OrderResponse response = OrderResponse.success(
                    orderRequest.getOrderId(),
                    orderRequest.getUserId(),
                    orderRequest.getTotalPrice()
            );
            response.setKafkaMessageId(sendResult.getRecordMetadata().offset() + "-" + 
                    sendResult.getRecordMetadata().partition());
            
            return response;
            
        } catch (Exception e) {
            log.error("Error placing order", e);
            throw new RuntimeException("Failed to place order: " + e.getMessage(), e);
        }
    }

    /**
     * Validate order request
     * @param orderRequest Order to validate
     * @throws IllegalArgumentException if validation fails
     */
    public void validateOrder(OrderRequest orderRequest) {
        if (orderRequest == null) {
            throw new IllegalArgumentException("Order request cannot be null");
        }
        if (orderRequest.getUserId() == null || orderRequest.getUserId().isEmpty()) {
            throw new IllegalArgumentException("User ID is required");
        }
        if (orderRequest.getItems() == null || orderRequest.getItems().isEmpty()) {
            throw new IllegalArgumentException("Items are required");
        }
        if (orderRequest.getTotalPrice() == null || orderRequest.getTotalPrice().signum() <= 0) {
            throw new IllegalArgumentException("Total price must be greater than 0");
        }
    }
}
EOF
cat > src/main/java/com/enduser/controller/OrderController.java << 'EOF'
package com.enduser.controller;

import com.enduser.dtos.OrderRequest;
import com.enduser.dtos.OrderResponse;
import com.enduser.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.HashMap;
import java.util.Map;

/**
 * REST Controller for Order operations
 * Handles HTTP requests for order placement and retrieval
 */
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {
    private final OrderService orderService;

    /**
     * Place a new order
     * @param orderRequest Order details
     * @return OrderResponse with order confirmation
     */
    @PostMapping
    public ResponseEntity<OrderResponse> placeOrder(@RequestBody OrderRequest orderRequest) {
        try {
            log.info("Received order request for user: {}", orderRequest.getUserId());
            
            // Validate order
            orderService.validateOrder(orderRequest);
            
            // Place order and publish to Kafka
            OrderResponse response = orderService.placeOrder(orderRequest);
            
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
            
        } catch (IllegalArgumentException e) {
            log.warn("Validation error: {}", e.getMessage());
            return ResponseEntity.badRequest()
                    .body(OrderResponse.error(e.getMessage()));
        } catch (Exception e) {
            log.error("Error placing order", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(OrderResponse.error("Internal server error: " + e.getMessage()));
        }
    }

    /**
     * Get order details (placeholder - actual implementation would fetch from DB)
     * @param orderId Order ID
     * @return Order details
     */
    @GetMapping("/{orderId}")
    public ResponseEntity<Map<String, Object>> getOrder(@PathVariable String orderId) {
        try {
            log.info("Fetching order: {}", orderId);
            
            Map<String, Object> response = new HashMap<>();
            response.put("orderId", orderId);
            response.put("status", "PLACED");
            response.put("message", "Order has been placed and sent for processing");
            response.put("timestamp", System.currentTimeMillis());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Error fetching order", e);
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Order not found");
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
        }
    }

    /**
     * Health check endpoint
     * @return Health status
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        Map<String, String> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "EndUser Service");
        response.put("timestamp", String.valueOf(System.currentTimeMillis()));
        return ResponseEntity.ok(response);
    }
}
EOF
cat > src/main/java/com/enduser/config/AppConstants.java << 'EOF'
package com.enduser.config;

/**
 * Application constants for EndUser service
 */
public class AppConstants {
    // Kafka Topics
    public static final String ORDER_PLACED_TOPIC = "order-placed-topic";
    
    // Kafka Consumer Groups
    public static final String ORDER_CONSUMER_GROUP = "order-group";
    
    // Application Settings
    public static final int KAFKA_PARTITIONS = 3;
    public static final int KAFKA_REPLICATION_FACTOR = 1;
    public static final long KAFKA_TIMEOUT_MS = 30000;
    
    // Logging
    public static final String LOGGER_NAME = "KafkaLearning-EndUser";
    
    private AppConstants() {
        // Prevent instantiation
    }
}
EOF
cat > src/main/resources/application.yml << 'EOF'
spring:
  application:
    name: EndUser-Service
  profiles:
    active: prod
  kafka:
    bootstrap-servers: kafka:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all
      retries: 3
      linger-ms: 10
      batch-size: 16384
      compression-type: snappy
    properties:
      linger.ms: 10
      batch.size: 16384
      compression.type: snappy

server:
  port: 8080
  servlet:
    context-path: /
  tomcat:
    max-threads: 100
    min-spare-threads: 10

logging:
  level:
    root: INFO
    com.enduser: DEBUG
    org.springframework.kafka: INFO
  pattern:
    console: "%d{yyyy-MM-dd HH:mm:ss} - %msg%n"
  file:
    name: logs/enduser.log
    max-size: 10MB
    max-history: 10

management:
  endpoints:
    web:
      exposure:
        include: health,metrics,info
  endpoint:
    health:
      show-details: always
  metrics:
    export:
      simple:
        enabled: true

EOF
cd ../DeliveryBoyApp && cat > src/main/java/com/deliveryboy/dtos/DeliveryRequest.java << 'EOF'
package com.deliveryboy.dtos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

/**
 * DTO for Delivery Request
 * Represents delivery location update
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeliveryRequest {
    private String orderId;
    private String location;
    private String deliveryBoyId;
    private String status;
    private LocalDateTime timestamp;
    private Double latitude;
    private Double longitude;
}
EOF
cat > src/main/java/com/deliveryboy/dtos/OrderMessage.java << 'EOF'
package com.deliveryboy.dtos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * DTO representing Order Message from Kafka topic
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderMessage {
    private String orderId;
    private String userId;
    private String items;
    private BigDecimal totalPrice;
    private LocalDateTime timestamp;
    private String deliveryAddress;
    private String phoneNumber;
}
EOF
cat > src/main/java/com/deliveryboy/service/DeliveryService.java << 'EOF'
package com.deliveryboy.service;

import com.deliveryboy.dtos.DeliveryRequest;
import com.deliveryboy.dtos.OrderMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import java.util.UUID;

/**
 * Service for handling delivery operations
 * Consumes order messages from Kafka and publishes delivery updates
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DeliveryService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private static final String DELIVERY_UPDATES_TOPIC = "delivery-updates-topic";

    /**
     * Consume order messages from Kafka topic
     * @param message JSON message containing order details
     * @param partition Kafka partition
     * @param offset Kafka offset
     */
    @KafkaListener(
            topics = "order-placed-topic",
            groupId = "delivery-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrder(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        try {
            log.info("Received message from partition: {}, offset: {}", partition, offset);
            
            // Deserialize message
            OrderMessage order = objectMapper.readValue(message, OrderMessage.class);
            
            log.info("Processing order: {} for user: {}", order.getOrderId(), order.getUserId());
            
            // Process the order
            processOrder(order);
            
            // Publish delivery update
            publishDeliveryUpdate(order);
            
            log.info("Order {} processed successfully", order.getOrderId());
            
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            log.error("JSON parsing error for message. Skipping: {}", message, e);
        } catch (Exception e) {
            log.error("Error consuming order message", e);
        }
    }

    /**
     * Process the order and assign delivery
     * @param order Order message
     */
    private void processOrder(OrderMessage order) {
        try {
            log.debug("Order processing started for orderId: {}", order.getOrderId());
            
            // Simulate order processing
            Thread.sleep(100);
            
            log.info("Order {} assigned for delivery", order.getOrderId());
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Order processing interrupted", e);
        }
    }

    /**
     * Publish delivery update to delivery-updates-topic
     * @param order Order details
     */
    private void publishDeliveryUpdate(OrderMessage order) {
        try {
            DeliveryRequest delivery = new DeliveryRequest();
            delivery.setOrderId(order.getOrderId());
            delivery.setLocation("Assigned to delivery network");
            delivery.setDeliveryBoyId("DB-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
            delivery.setStatus("ASSIGNED");
            delivery.setTimestamp(java.time.LocalDateTime.now());
            
            String deliveryJson = objectMapper.writeValueAsString(delivery);
            
            kafkaTemplate.send(DELIVERY_UPDATES_TOPIC, order.getUserId(), deliveryJson);
            
            log.info("Delivery update published for order: {}", order.getOrderId());
            
        } catch (Exception e) {
            log.error("Error publishing delivery update", e);
        }
    }

    /**
     * Update delivery location
     * @param deliveryRequest Delivery update details
     */
    public void updateDeliveryLocation(DeliveryRequest deliveryRequest) {
        try {
            log.info("Updating delivery location for order: {}", deliveryRequest.getOrderId());
            
            String deliveryJson = objectMapper.writeValueAsString(deliveryRequest);
            kafkaTemplate.send(DELIVERY_UPDATES_TOPIC, deliveryRequest.getOrderId(), deliveryJson);
            
            log.info("Delivery location updated for order: {}", deliveryRequest.getOrderId());
            
        } catch (Exception e) {
            log.error("Error updating delivery location", e);
            throw new RuntimeException("Failed to update delivery location", e);
        }
    }
}
EOF
cat > src/main/java/com/deliveryboy/controller/DeliveryController.java << 'EOF'
package com.deliveryboy.controller;

import com.deliveryboy.dtos.DeliveryRequest;
import com.deliveryboy.service.DeliveryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.HashMap;
import java.util.Map;

/**
 * REST Controller for Delivery operations
 * Handles delivery location updates and status retrieval
 */
@RestController
@RequestMapping("/api/delivery")
@RequiredArgsConstructor
@Slf4j
public class DeliveryController {
    private final DeliveryService deliveryService;

    /**
     * Update delivery location
     * @param deliveryRequest Delivery location update
     * @return Response with update status
     */
    @PostMapping("/update")
    public ResponseEntity<Map<String, Object>> updateDelivery(
            @RequestBody DeliveryRequest deliveryRequest) {
        try {
            log.info("Received delivery update for order: {}", deliveryRequest.getOrderId());
            
            // Update delivery location
            deliveryService.updateDeliveryLocation(deliveryRequest);
            
            Map<String, Object> response = new HashMap<>();
            response.put("orderId", deliveryRequest.getOrderId());
            response.put("status", "UPDATED");
            response.put("location", deliveryRequest.getLocation());
            response.put("deliveryBoyId", deliveryRequest.getDeliveryBoyId());
            response.put("message", "Delivery location updated successfully");
            response.put("timestamp", System.currentTimeMillis());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Error updating delivery", e);
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Failed to update delivery");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Get delivery status for an order
     * @param orderId Order ID
     * @return Delivery status
     */
    @GetMapping("/status/{orderId}")
    public ResponseEntity<Map<String, Object>> getDeliveryStatus(
            @PathVariable String orderId) {
        try {
            log.info("Fetching delivery status for order: {}", orderId);
            
            Map<String, Object> response = new HashMap<>();
            response.put("orderId", orderId);
            response.put("status", "IN_PROGRESS");
            response.put("location", "On the way");
            response.put("estimatedTime", "30 minutes");
            response.put("timestamp", System.currentTimeMillis());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Error fetching delivery status", e);
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Delivery not found");
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
        }
    }

    /**
     * Health check endpoint
     * @return Health status
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        Map<String, String> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "DeliveryBoyApp Service");
        response.put("timestamp", String.valueOf(System.currentTimeMillis()));
        return ResponseEntity.ok(response);
    }
}
EOF
cat > src/main/resources/application.yml << 'EOF'
spring:
  application:
    name: DeliveryBoyApp-Service
  profiles:
    active: prod
  kafka:
    bootstrap-servers: kafka:9092
    consumer:
      bootstrap-servers: kafka:9092
      group-id: delivery-group
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      max-poll-records: 100
      session-timeout-ms: 30000
      heartbeat-interval-ms: 10000
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all
      retries: 3
    listener:
      type: batch
      concurrency: 3
      poll-timeout: 3000

server:
  port: 8081
  servlet:
    context-path: /
  tomcat:
    max-threads: 100
    min-spare-threads: 10

logging:
  level:
    root: INFO
    com.deliveryboy: DEBUG
    org.springframework.kafka: INFO
  pattern:
    console: "%d{yyyy-MM-dd HH:mm:ss} - %msg%n"
  file:
    name: logs/deliveryboy.log
    max-size: 10MB
    max-history: 10

management:
  endpoints:
    web:
      exposure:
        include: health,metrics,info
  endpoint:
    health:
      show-details: always
  metrics:
    export:
      simple:
        enabled: true

EOF

git status
git diff --cached
ls -la EndUser/src/main/java/com/enduser/dtos/


#### Problem: Serialization error when sending messages
```
org.springframework.kafka.KafkaException: Send failed
```

**Solution**:
- Verify serializer class in KafkaConfigs.java
- Ensure message object is serializable
- Check application.yml for correct serializer configuration:
```yaml
spring:
  kafka:
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
```

#### Problem: JSON parsing error in consumer
```
java.io.IOException: Unexpected end-of-input in field name
```

**Solution**:
- Verify message format is valid JSON
- Check deserializer class configuration
- Add error handler in @KafkaListener:
```java
@KafkaListener(topics = "order-placed-topic", groupId = "delivery-group",
  errorHandler = "kafkaErrorHandler")
public void consume(String message) {
    // Process message
}
```

### 4. Spring Boot Application Issues

#### Problem: Application fails to start
```
SpringApplication run failed with exception
```

**Solution**:
- Check application.yml syntax
- Verify all required properties are set
- Check logs: `./mvnw spring-boot:run | grep -A 10 ERROR`
- Ensure port is not already in use:
```bash
# Kill process on port 8080
lsof -i :8080 | grep LISTEN | awk '{print $2}' | xargs kill -9
```

#### Problem: "Bean not found" exception
```
Unsatisfied dependency expressed through constructor parameter
```

**Solution**:
- Ensure component is properly annotated (@Service, @Component, @Repository)
- Check that bean is in Spring Boot classpath
- Verify no circular dependencies
- Check component-scan configuration

### 5. Docker Issues

#### Problem: Docker Compose fails to start

**Solution**:
- Clean up containers: `docker-compose down -v`
- Rebuild images: `docker-compose build --no-cache`
- Restart: `docker-compose up -d`

#### Problem: Port already in use

**Solution**:
- Check what's using the port:
```bash
lsof -i :9092  # For Kafka
lsof -i :2181  # For Zookeeper
```
- Kill the process or change port in docker-compose.yml

### 6. Message Not Being Consumed

#### Problem: Messages sit in topic, consumer doesn't process them

**Solution**:
1. Check consumer is running: `docker-compose ps`
2. Verify consumer group is active:
```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --list
```
3. Check consumer lag:
```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group delivery-group --describe
```
4. Check application logs for exceptions
5. Verify @KafkaListener is properly configured

### 7. High Memory Usage

#### Problem: Docker containers consuming too much memory

**Solution**:
- Check memory usage: `docker stats`
- Limit memory in docker-compose.yml:
```yaml
services:
  kafka:
    mem_limit: 2g
```
- Reduce Kafka heap size:
```yaml
environment:
  KAFKA_HEAP_OPTS: "-Xms512M -Xmx512M"
```

### 8. Network Issues

#### Problem: Containers can't communicate with each other

**Solution**:
- Check network: `docker network ls`
- Inspect network: `docker network inspect <network-name>`
- Ensure services are on same network in docker-compose.yml
- Use service names (not localhost) for internal communication

## Debugging Tips

### 1. Check Logs
```bash
# Application logs
docker logs <service-name> -f

# Kafka logs
docker logs kafka -f

# Zookeeper logs
docker logs zookeeper -f
```

### 2. Enter Container
```bash
docker exec -it <container-name> bash
```

### 3. Monitor Message Flow
```bash
# Read messages from topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order-placed-topic \
  --from-beginning
```

### 4. Check Health
```bash
# Application health
curl http://localhost:8080/actuator/health
curl http://localhost:8081/actuator/health

# Kafka metrics
curl http://localhost:8080/actuator/metrics/kafka.consumer.lag
```

### 5. Test Connectivity
```bash
# From EndUser to Kafka
docker exec enduser nc -zv kafka 9092

# From DeliveryBoyApp to Kafka
docker exec deliveryboy nc -zv kafka 9092
```

## Performance Issues

### Slow Message Processing
- Increase consumer threads: Configure in KafkaConfig.java
- Batch processing: Use `batch_max_bytes`
- Optimize business logic in service classes

### High Latency
- Check consumer lag
- Increase partition count
- Tune batch.size and linger.ms in producer config

## Database Issues (Future)

When database is added:
- Ensure database is running and accessible
- Check connection pool settings
- Verify schema is created
- Check for connection leaks

## Still Having Issues?

1. Check existing GitHub issues: https://github.com/shrihari7396/KafkaLearning/issues
2. Review logs carefully for error messages
3. Enable DEBUG logging in application.yml:
```yaml
logging:
  level:
    root: DEBUG
    org.springframework.kafka: DEBUG
```
4. Create detailed issue report with:
   - Error messages
   - Steps to reproduce
   - Docker/Java versions
   - Full logs

