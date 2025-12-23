# KafkaLearning - Quick Start Guide

## Prerequisites
- Docker & Docker Compose
- JDK 17+
- Maven 3.8+
- curl or Postman for testing

## Step 1: Start Kafka Infrastructure

```bash
# Navigate to project root
cd /path/to/KafkaLearning

# Start Kafka, Zookeeper
docker-compose up -d

# Wait 15-20 seconds for Kafka to fully initialize
sleep 20

# Verify containers are running
docker-compose ps
```

## Step 2: Run EndUser Service (Producer)

```bash
# Open a new terminal
cd EndUser

# Build and run
./mvnw spring-boot:run

# Expected output:
# 2024-XX-XX XX:XX:XX - Started EndUserApplication in X.XXX seconds
# Server is running on http://localhost:8080
```

## Step 3: Run DeliveryBoyApp Service (Consumer)

```bash
# Open another new terminal
cd DeliveryBoyApp

# Build and run
./mvnw spring-boot:run

# Expected output:
# 2024-XX-XX XX:XX:XX - Started DeliveryBoyAppApplication in X.XXX seconds  
# Server is running on http://localhost:8081
# Listening to Kafka topic: order-placed-topic
```

## Step 4: Test the System

### 4.1 Place an Order

```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "USER001",
    "items": "Biryani, Samosa, Chai",
    "totalPrice": 450.50,
    "deliveryAddress": "123 Main Street, Mumbai",
    "phoneNumber": "+91-9876543210"
  }'
```

**Expected Response (201 Created):**
```json
{
  "orderId": "ORD-ABC12345",
  "userId": "USER001",
  "status": "CREATED",
  "message": "Order placed successfully",
  "totalPrice": 450.50,
  "createdAt": "2024-12-23T12:00:00",
  "kafkaMessageId": "0-0"
}
```

### 4.2 Check Delivery Status

```bash
curl http://localhost:8081/api/delivery/status/ORD-ABC12345
```

**Expected Response (200 OK):**
```json
{
  "orderId": "ORD-ABC12345",
  "status": "IN_PROGRESS",
  "location": "On the way",
  "estimatedTime": "30 minutes",
  "timestamp": 1703322000000
}
```

### 4.3 Update Delivery Location

```bash
curl -X POST http://localhost:8081/api/delivery/update \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "ORD-ABC12345",
    "location": "Approaching your location",
    "deliveryBoyId": "DB-XYZ789",
    "status": "NEARBY",
    "latitude": 19.0760,
    "longitude": 72.8777
  }'
```

**Expected Response (200 OK):**
```json
{
  "orderId": "ORD-ABC12345",
  "status": "UPDATED",
  "location": "Approaching your location",
  "deliveryBoyId": "DB-XYZ789",
  "message": "Delivery location updated successfully",
  "timestamp": 1703322000000
}
```

### 4.4 Health Checks

```bash
# Check EndUser service health
curl http://localhost:8080/api/orders/health

# Check DeliveryBoyApp service health
curl http://localhost:8081/api/delivery/health
```

## Step 5: Monitor Kafka Messages

### In a new terminal, watch order messages:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order-placed-topic \
  --from-beginning
```

### In another terminal, watch delivery updates:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic delivery-updates-topic \
  --from-beginning
```

## Step 6: View Service Logs

### EndUser Service Logs:
```bash
tail -f logs/enduser.log
```

### DeliveryBoyApp Service Logs:
```bash
tail -f logs/deliveryboy.log
```

## Troubleshooting

### Kafka Connection Refused
```bash
# Check if Kafka is running
docker-compose ps

# Check Kafka logs
docker logs kafka

# Restart Kafka
docker-compose restart kafka
```

### Port Already in Use
```bash
# Check what's using port 8080
lsof -i :8080

# Check what's using port 8081
lsof -i :8081

# Kill the process if needed
kill -9 <PID>
```

### Services Not Starting
```bash
# Check if Java is installed
java -version

# Check Maven is available
mvn -version

# Check logs
cd EndUser && ./mvnw spring-boot:run -X
```

## Test Scenarios

### Scenario 1: Complete Order Flow
1. Place order via `/api/orders`
2. Monitor Kafka logs - order should appear in order-placed-topic
3. Check DeliveryBoyApp logs - order should be consumed and processed
4. Check delivery status via `/api/delivery/status/{orderId}`
5. Monitor Kafka logs - delivery update should appear in delivery-updates-topic

### Scenario 2: Invalid Order
```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "",
    "items": "Item",
    "totalPrice": -100
  }'
```

Expected: 400 Bad Request with error messages

### Scenario 3: Multiple Orders
```bash
for i in {1..5}; do
  curl -X POST http://localhost:8080/api/orders \
    -H "Content-Type: application/json" \
    -d '{
      "userId": "USER'$i'",
      "items": "Item'$i'",
      "totalPrice": '$((100 * i))'
    }'
  sleep 1
done
```

Monitor the messages in Kafka to see all 5 orders being processed

## Performance Testing

### Send 100 Orders
```bash
for i in {1..100}; do
  curl -X POST http://localhost:8080/api/orders \
    -H "Content-Type: application/json" \
    -d '{
      "userId": "PERF-'$i'",
      "items": "Item'$i'",
      "totalPrice": '$((RANDOM % 1000))'
    }' &
done
```

Expected: All 100 messages should be published and consumed successfully

## Cleanup

```bash
# Stop services (Ctrl+C in terminals running services)

# Stop Docker containers
docker-compose down

# Remove volumes (if needed)
docker-compose down -v

# Clean build artifacts
cd EndUser && mvn clean
cd ../DeliveryBoyApp && mvn clean
```

## Next Steps

- Read ARCHITECTURE.md for detailed design
- Read IMPLEMENTATION.md for code details
- Read API.md for endpoint documentation
- Check CONTRIBUTING.md for development guidelines
- See TROUBLESHOOTING.md for common issues

