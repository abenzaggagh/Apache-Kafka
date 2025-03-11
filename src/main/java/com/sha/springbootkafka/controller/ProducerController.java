package com.sha.springbootkafka.controller;

import com.sha.springbootkafka.model.MessageEntity;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j

@RestController
@RequestMapping("api/producer")

public class ProducerController {

    @Value("${kafka.first-topic}")
    private String FIRST_TOPIC;

    @Value("${kafka.second-topic}")
    private String SECOND_TOPIC;

    @Value("${kafka.transactional-topic}")
    private String TRANSACTIONAL_TOPIC;

    private final KafkaTemplate<String, Object> kafkaProducerTemplate;

    private final KafkaTemplate<String, Object> kafkaTransactionalProducerTemplate;

    @Autowired
    public ProducerController(KafkaTemplate<String, Object> kafkaProducerTemplate,
                              @Qualifier("kafkaTransactionalProducerTemplate")
                              KafkaTemplate<String, Object> kafkaTransactionalProducerTemplate) {
        this.kafkaProducerTemplate = kafkaProducerTemplate;
        this.kafkaTransactionalProducerTemplate = kafkaTransactionalProducerTemplate;
    }

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
            public void onFailure(@NonNull Throwable ex) {
                log.error("Sent message failed", ex);
            }
        });

        return ResponseEntity.ok().body(messageEntity);
    }

    @PostMapping("send-with-key/{key}")
    public ResponseEntity<?> sendMessageWithKey(@PathVariable String key) throws ExecutionException, InterruptedException {

        MessageEntity messageEntity = new MessageEntity("record-key", LocalDateTime.now());

        ListenableFuture<SendResult<String, Object>> future = kafkaProducerTemplate.send(SECOND_TOPIC, key, messageEntity);

        SendResult<String, Object> result = future.get();

        log.info("Sent key message with offset : {}, partition: {}", result.getRecordMetadata().offset(), result.getRecordMetadata().partition());

        return ResponseEntity.ok().body(messageEntity);
    }

    @PostMapping("transaction/{key}")
    public ResponseEntity<?> sendTransactionalMessage(@PathVariable String key) {
        List<MessageEntity> messageEntities = new ArrayList<>();

        kafkaTransactionalProducerTemplate.executeInTransaction(kafkaOperations -> {
            String[] keyList = key.split(",");

            for(String str: keyList) {
                if ("success".equals(str)) {
                    MessageEntity messageEntity = new MessageEntity(str, LocalDateTime.now());

                    kafkaOperations.send(TRANSACTIONAL_TOPIC, messageEntity);
                    messageEntities.add(messageEntity);
                } else {
                    throw new RuntimeException();
                }
            }

            return null;
        });

        return ResponseEntity.ok().body(messageEntities);
    }

}
