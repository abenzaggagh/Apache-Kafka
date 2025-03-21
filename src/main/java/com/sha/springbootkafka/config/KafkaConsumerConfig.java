package com.sha.springbootkafka.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
// Required for Kafka Listener
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String CONSUMER_BROKERS;

    @Bean
    public Consumer<String, Object> manualConsumer() {
        return consumerFactory("consumerGroupManual").createConsumer();
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory("consumerGroup1"));
        factory.setAutoStartup(true);
        factory.setConcurrency(1);

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> priorityKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();

        factory.setRecordFilterStrategy(consumerRecord -> !"priority".equals(consumerRecord.key()));
        factory.setConsumerFactory(consumerFactory("consumerPriorityGroup"));
        factory.setAutoStartup(true);
        factory.setConcurrency(1);

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> nonPriorityKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();

        factory.setRecordFilterStrategy(consumerRecord -> "priority".equals(consumerRecord.key()));
        factory.setConsumerFactory(consumerFactory("consumerNonPriorityGroup"));
        factory.setAutoStartup(true);
        factory.setConcurrency(1);

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> errorHandlerKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory("consumerErrorHandlerGroup"));
        factory.setErrorHandler(new SeekToCurrentErrorHandler(new FixedBackOff(500, 2)));
        // Retry 2 times with 500 ms interval
        factory.setAutoStartup(true);
        factory.setConcurrency(1);

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> transactionalKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory("consumerTransactionalGroup"));
        factory.setAutoStartup(true);
        factory.setConcurrency(1);

        return factory;
    }

    private ConsumerFactory<String, Object> consumerFactory(String groupID) {
        Map<String, Object> properties = new HashMap<>();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CONSUMER_BROKERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_commited");

        JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>();
        jsonDeserializer.addTrustedPackages("*");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, jsonDeserializer);

        return new DefaultKafkaConsumerFactory<>(properties, new StringDeserializer(), jsonDeserializer);
    }

}
