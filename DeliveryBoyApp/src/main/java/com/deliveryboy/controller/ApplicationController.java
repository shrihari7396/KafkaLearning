package com.deliveryboy.controller;

import com.deliveryboy.dtos.RequestDto;
import com.deliveryboy.service.KafkaService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@Slf4j
public class ApplicationController {

    private final KafkaService kafkaService;

    @PostMapping("/update")
    public ResponseEntity<?> updateLocation(@RequestBody RequestDto location) {
        for(int i = 0; i < 100000; i++) {
            this.kafkaService.updateLocation(location.getLocation());
        }
        log.info("Update location successfully...");
        return ResponseEntity.ok().build();
    }

}
