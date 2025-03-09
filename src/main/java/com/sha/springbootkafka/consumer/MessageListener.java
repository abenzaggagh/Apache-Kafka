package com.sha.springbootkafka.consumer;

import com.sha.springbootkafka.model.MessageEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j

@Component
public class MessageListener {

    @KafkaListener(topics = "${kafka.first-topic}", containerFactory = "kafkaListenerContainerFactory")
    public void listenFirstTopic(Object record) {
        log.info("Received message in group1: {}", record);
    }

    @KafkaListener(topics = "${kafka.first-topic}", groupId = "consumerGroup2", containerFactory = "kafkaListenerContainerFactory")
    public void listenFirstTopic2(Object record) {
        log.info("Received message in group2: {}", record);
    }

    @KafkaListener(topics = "${kafka.first-topic}", groupId = "consumerGroup3", containerFactory = "kafkaListenerContainerFactory")
    public void listenFirstTopic3(ConsumerRecord<String, MessageEntity> consumerRecord,
                                  @Payload MessageEntity messageEntity,
                                  @Header(KafkaHeaders.GROUP_ID) String groupID,
                                  @Header(KafkaHeaders.OFFSET) int offset,
                                  @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partitionID) {
        log.info("-- Received message in group3: {} --", consumerRecord);
        log.info("   Received message : {}", messageEntity);
        log.info("   Received message in : {}", groupID);
        log.info("   Received message with offset : {}", offset);
        log.info("   Received message in partition : {}", partitionID);
    }

    @KafkaListener(
            topics = "${kafka.first-topic}",
            groupId = "consumerGroup4",
            containerFactory = "kafkaListenerContainerFactory",
            topicPartitions = {
                    @TopicPartition(topic = "${kafka.first-topic}", partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0"))
            }
    )
    public void listenFirstTopicFromBeginning(Object record) {
        log.info("Received message in group from beginning: {}", record);
    }

}
