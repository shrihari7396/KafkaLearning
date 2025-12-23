# Contributing to KafkaLearning

## Getting Started

### Prerequisites
- JDK 17 or higher
- Maven 3.8+
- Docker & Docker Compose
- Git

### Setup Development Environment

1. Clone the repository:
```bash
git clone https://github.com/shrihari7396/KafkaLearning.git
cd KafkaLearning
```

2. Create a new branch for your feature:
```bash
git checkout -b feature/your-feature-name
```

3. Start Kafka and Zookeeper:
```bash
docker-compose up -d
```

4. Build the project:
```bash
cd EndUser && mvn clean install
cd ../DeliveryBoyApp && mvn clean install
```

5. Run the services:
```bash
# Terminal 1 - EndUser Service
cd EndUser && ./mvnw spring-boot:run

# Terminal 2 - DeliveryBoyApp Service  
cd DeliveryBoyApp && ./mvnw spring-boot:run
```

## Code Structure

### EndUser Service
```
EndUser/
├── src/main/java/com/enduser/
├── src/main/resources/
└── pom.xml
```

### DeliveryBoyApp Service
```
DeliveryBoyApp/
├── src/main/java/com/deliveryboy/
├── src/main/resources/
└─┐ pom.xml
```

## Coding Standards

### Java Code Style
- Follow Google Java Style Guide
- Use meaningful variable and method names
- Maximum line length: 120 characters
- Use 4 spaces for indentation

### Kafka Integration
- Always configure serializers/deserializers
- Use KafkaTemplate for sending messages
- Implement @KafkaListener for consuming messages
- Handle serialization exceptions gracefully

### Spring Boot Best Practices
- Use dependency injection with @Autowired or constructor injection
- Implement proper exception handling
- Use logging (SLF4J) instead of System.out
- Configure application.yml for environment-specific settings

## Testing

### Unit Tests
Located in `src/test/java` directory

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=KafkaServiceTest
```

### Integration Tests
Test Kafka integration using embedded Kafka

### Manual Testing
Use curl or Postman to test endpoints:

```bash
# Order Placement
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "ORD123",
    "userId": "USER456",
    "items": "Pizza, Coke",
    "totalPrice": 500
  }'

# Check Delivery Status
curl http://localhost:8081/api/delivery/status/ORD123
```

## Git Workflow

1. Create feature branch: `git checkout -b feature/my-feature`
2. Make commits with clear messages: `git commit -m "Add Kafka listener"`
3. Push to your fork: `git push origin feature/my-feature`
4. Create Pull Request with detailed description
5. Address code review comments
6. Merge when approved

## Common Development Tasks

### Adding a New Kafka Topic
1. Define topic in `KafkaConfig.java` using `@Bean` annotation
2. Configure producer/consumer in `application.yml`
3. Create message DTOs in `dtos` folder
4. Write test cases

### Adding a New REST Endpoint
1. Create controller method in respective controller
2. Add service logic
3. Define request/response DTOs
4. Write unit tests
5. Document in API.md

### Debugging Kafka Messages

Check message flow:
```bash
# Connect to Kafka container
docker exec -it <kafka-container> bash

# List topics
kafka-topics --list --bootstrap-server localhost:9092

# Read messages from topic
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic order-placed-topic --from-beginning
```

## Documentation

- Update README.md for user-facing changes
- Update API.md for API changes
- Update ARCHITECTURE.md for architectural changes
- Add JavaDoc comments for public methods

## Performance Considerations

- Use connection pooling
- Batch Kafka messages when possible
- Monitor consumer lag
- Optimize serialization/deserialization
- Use async processing for long-running operations

## Security Guidelines

- Never commit sensitive data (passwords, API keys)
- Validate all user inputs
- Use HTTPS in production
- Implement proper authentication
- Regular dependency updates for security patches

## Issue Reporting

When reporting issues, include:
- Detailed description of the problem
- Steps to reproduce
- Expected vs actual behavior
- Environment details (OS, Java version, etc.)
- Relevant logs or error messages

## Pull Request Guidelines

- Keep PRs focused and reasonably sized
- Write descriptive PR title and description
- Link related issues
- Ensure all tests pass
- Update documentation
- Request review from maintainers

## Questions?

Feel free to open an issue or contact the maintainers.

