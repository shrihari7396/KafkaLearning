
# KafkaLearning

This repository contains a simple Kafka learning project with two Spring Boot microservices: **EndUser** (producer) and **DeliveryBoyApp** (consumer). [page:1][page:1]

## Architecture

- **EndUser**  
  - Spring Boot service acting as a Kafka **producer**. [page:1]  
  - Publishes messages to a configured Kafka topic (e.g. when a user places an order).  

- **DeliveryBoyApp**  
  - Spring Boot service acting as a Kafka **consumer**. [page:1]  
  - Listens to the same Kafka topic and processes the incoming messages (e.g. simulating delivery updates).  

- **Kafka & Zookeeper (Docker)**  
  - Brought up using `docker-compose.yml`. [page:1]  
  - Both services connect to this Kafka broker.

## Prerequisites

- Java 17 or compatible JDK installed.  
- Maven installed (or use the provided `mvnw` / `mvnw.cmd` in each module). [page:2][page:3]  
- Docker and Docker Compose installed to run Kafka via `docker-compose.yml`. [page:1]

## Project Structure

```
KafkaLearning/
├─ DeliveryBoyApp/     # Consumer service (Spring Boot)
│  ├─ src/             # Java source code
│  └─ pom.xml
├─ EndUser/            # Producer service (Spring Boot)
│  ├─ src/
│  └─ pom.xml
└─ docker-compose.yml  # Kafka + Zookeeper stack
```
[page:1][page:2][page:3]

## How to Run

1. **Start Kafka using Docker Compose**

   ```
   docker-compose up -d
   ```

2. **Run EndUser (producer)**

   ```
   cd EndUser
   ./mvnw spring-boot:run   # Linux/Mac
   # or
   mvnw.cmd spring-boot:run # Windows
   ```

3. **Run DeliveryBoyApp (consumer)**

   ```
   cd DeliveryBoyApp
   ./mvnw spring-boot:run   # Linux/Mac
   # or
   mvnw.cmd spring-boot:run # Windows
   ```

## Usage

- Send a request to the **EndUser** service endpoint that publishes a message to Kafka (e.g. an order payload).  
- Observe **DeliveryBoyApp** logs to see the consumed messages and processing output.  
- Exact REST endpoints and payloads can be checked under each module’s `src/main/java` and `application.yml` / `application.properties`. [page:2][page:3]

## Learning Goals

This project is intended to:

- Understand **Kafka fundamentals**: topics, producers, consumers, and message flow.  
- Learn how to integrate **Spring Boot with Kafka** in a microservice setup.  
- Practice running Kafka locally using **Docker Compose**. [page:1]

## Future Improvements

- Add detailed API documentation (Swagger/OpenAPI).  
- Add retry and error handling logic for consumer failures.  
- Implement message keys and partitions for more realistic scenarios.  
- Add tests for producer and consumer flows.
