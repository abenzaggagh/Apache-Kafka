package com.sha.springbootkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import javax.annotation.PostConstruct;

/**
 * @author sa
 * @date 14.11.2021
 * @time 13:37
 */
@Configuration
public class KafkaTopicConfig
{
    @Value("${kafka.first-topic}")
    private String FIRST_TOPIC;

    @Value("${kafka.second-topic}")
    private String SECOND_TOPIC;

    @Value("${kafka.transactional-topic}")
    private String TRANSACTIONAL_TOPIC;

    @Autowired
    private KafkaAdmin kafkaAdmin;

    private NewTopic firstTopic()
    {
        return TopicBuilder.name(FIRST_TOPIC)
                .partitions(2)
                .replicas(2) //Cannot be larger than available brokers(2)
                .config(TopicConfig.RETENTION_MS_CONFIG, "1000000000")
                .build();
    }

    private NewTopic secondTopic()
    {
        return TopicBuilder.name(SECOND_TOPIC)
                .partitions(3)
                .replicas(2) //Cannot be larger than available brokers(2)
                .build();
    }

    private NewTopic transactionalTopic()
    {
        return TopicBuilder.name(TRANSACTIONAL_TOPIC)
                .partitions(3)
                .replicas(2) //Cannot be larger than available brokers(2)
                .build();
    }

    @PostConstruct
    public void init()
    {
        kafkaAdmin.createOrModifyTopics(firstTopic());
        kafkaAdmin.createOrModifyTopics(secondTopic());
        kafkaAdmin.createOrModifyTopics(transactionalTopic());
    }
}
