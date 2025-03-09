package com.sha.springbootkafka.controller;

import com.sha.springbootkafka.consumer.ManualConsumerService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/consumer")

@RequiredArgsConstructor
public class ConsumerController {

    @Value("${kafka.first-topic}")
    private String FIRST_TOPIC;

    private final ManualConsumerService manualConsumerService;

    @GetMapping("manual")
    public ResponseEntity<?> getMessagesManually(
            @RequestParam(value = "partition", required = false, defaultValue = "0") Integer partition,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset
    ) {
        return ResponseEntity.ok(manualConsumerService.receiveMessages(FIRST_TOPIC, partition, offset));
    }

}
