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

/**
 * @author sa
 * @date 14.11.2021
 * @time 14:55
 */
@Slf4j
@Component
public class MessageListener
{
    @KafkaListener(topics = "${kafka.first-topic}", containerFactory = "firstKafkaListenerContainerFactory")
    public void listenFirstTopic1(Object record)
    {
        log.info("Received1 message in group1: {}", record);
    }


    @KafkaListener(topics = "${kafka.first-topic}", groupId = "consumerGroup2", containerFactory = "firstKafkaListenerContainerFactory")
    public void listenFirstTopic2(Object record)
    {
        log.info("Received2 message in group1: {}", record);
    }

    @KafkaListener(topics = "${kafka.first-topic}", groupId = "consumerGroup3",
            containerFactory = "firstKafkaListenerContainerFactory")
    public void listenFirstTopicWithDetails(ConsumerRecord<String, MessageEntity> consumerRecord,
                                            @Payload MessageEntity messageEntity,
                                            @Header(KafkaHeaders.GROUP_ID) String groupId,
                                            @Header(KafkaHeaders.OFFSET) int offset,
                                            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition)
    {
        log.info("----------Received message------------");
        log.info("Record: {}", consumerRecord);
        log.info("Message: {}", messageEntity);
        log.info("GroupId: {}", groupId);
        log.info("Offset: {}", offset);
        log.info("Partition: {}", partition);
        log.info("<----------Received message------------>");
    }

    @KafkaListener(topics = "${kafka.first-topic}", groupId = "consumerGroup4",
            containerFactory = "firstKafkaListenerContainerFactory",
    topicPartitions = {@TopicPartition(topic = "${kafka.first-topic}",
            partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0"))})
    public void listenFirstTopicFromBeginning(Object record)
    {
        log.info("Received messages from beginning in consumerGroup4: {}", record);
    }


    @KafkaListener(topics = "${kafka.second-topic}", containerFactory = "priority1KafkaListenerContainerFactory")
    public void listenPriorityTopic(Object record)
    {
        log.info("Received urgent-message: {}", record);
    }

    @KafkaListener(topics = "${kafka.second-topic}", containerFactory = "otherPriorityKafkaListenerContainerFactory")
    public void listenNoPriorityTopic(Object record)
    {
        log.info("Received not-urgent-message: {}", record);
    }

    @KafkaListener(topics = "${kafka.second-topic}", containerFactory = "errorHandlerKafkaListenerContainerFactory")
    public void listenErrorHandler(Object record)
    {
        log.info("Received error-handler message: {}", record);

        throw new RuntimeException();
    }

    @KafkaListener(topics = "${kafka.transactional-topic}", containerFactory = "transactionalKafkaListenerContainerFactory")
    public void listenTransactionalTopic(Object record)
    {
        log.info("Received transactional message: {}", record);
    }
}
