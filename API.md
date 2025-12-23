# KafkaLearning - Comprehensive API Documentation

## Overview
This document provides detailed API documentation for the KafkaLearning microservices project demonstrating Apache Kafka with Spring Boot.

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [EndUser Service (Producer)](#enduser-service-producer)
3. [DeliveryBoyApp Service (Consumer)](#deliveryboyapp-service-consumer)
4. [Kafka Topics](#kafka-topics)
5. [Error Handling](#error-handling)
6. [Example Workflows](#example-workflows)

## Architecture Overview
The system uses a producer-consumer pattern with Apache Kafka:
- **EndUser Service**: Produces order messages
- **DeliveryBoyApp Service**: Consumes messages and updates delivery status
- **Kafka**: Message broker connecting both services
