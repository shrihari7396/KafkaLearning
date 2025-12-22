package com.enduser.enduser.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

@Configuration
@Slf4j
public class KafkaConfigs {

    @KafkaListener(topics = AppConstants.LOCATION_TOPIC_NAME,  groupId = AppConstants.GROUP_ID)
    public void listen() {
        log.info("Received Kafka message from topic: {}", AppConstants.LOCATION_TOPIC_NAME);
    }
}
