package com.sha.springbootkafka.controller;

import com.sha.springbootkafka.model.MessageEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;

@Slf4j

@RestController
@RequestMapping("api/producer")

@RequiredArgsConstructor
public class ProducerController {

    @Value("${kafka.first-topic}")
    private String FIRST_TOPIC;

    private final KafkaTemplate<String, Object> kafkaProducerTemplate;

    @PostMapping("send")
    public ResponseEntity<?> sendMessage() throws ExecutionException, InterruptedException {

        MessageEntity messageEntity = new MessageEntity("default", LocalDateTime.now());

        ListenableFuture<SendResult<String, Object>> future = kafkaProducerTemplate.send(FIRST_TOPIC, messageEntity);

        SendResult<String, Object> result = future.get();

        log.info("Sent message with offset : {}, partition: {}", result.getRecordMetadata().offset(), result.getRecordMetadata().partition());

        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, Object> result) {
                log.info("Sent message with offset : {}, partition: {}", result.getRecordMetadata().offset(), result.getRecordMetadata().partition());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error("Sent message failed", ex);
            }
        });

        return ResponseEntity.ok().body(messageEntity);
    }



}
