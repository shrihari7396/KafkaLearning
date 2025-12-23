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

### 3. Serialization Issues

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

