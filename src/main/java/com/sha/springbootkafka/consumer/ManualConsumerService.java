package com.sha.springbootkafka.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
@Service

@RequiredArgsConstructor
public class ManualConsumerService {

    private final Consumer<String, Object> manualConsumer;

    public List<Object> receiveMessages(String topicName, int partition, int offset) {
        TopicPartition topicPartition = new TopicPartition(topicName, partition);

        manualConsumer.assign(List.of(topicPartition));
        manualConsumer.seek(topicPartition, offset);

        ConsumerRecords<String, Object> records = manualConsumer.poll(Duration.ofMillis(1000));

        records.forEach(record -> log.info("Record: {}", record));

        manualConsumer.unsubscribe();

        return StreamSupport.stream(records.spliterator(), false)
                .map(ConsumerRecord::value)
                .collect(Collectors.toList());
    }

}
