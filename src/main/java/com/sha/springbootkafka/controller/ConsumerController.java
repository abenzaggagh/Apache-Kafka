package com.sha.springbootkafka.controller;

import com.sha.springbootkafka.consumer.ManualConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author sa
 * @date 14.11.2021
 * @time 14:42
 */
@RestController
@RequestMapping("api/consumer")//pre-path
public class ConsumerController
{
    @Value("${kafka.first-topic}")
    private String FIRST_TOPIC;

    @Autowired
    private ManualConsumerService manualConsumerService;

    @GetMapping("manual")//api/consumer/manual?partition=&offset=
    public ResponseEntity<?> getMessagesManually(
            @RequestParam(value = "partition", required = false, defaultValue = "0") Integer partition,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset
    )
    {
        return ResponseEntity.ok(manualConsumerService.receiveMessages(FIRST_TOPIC, partition, offset));
    }
}
