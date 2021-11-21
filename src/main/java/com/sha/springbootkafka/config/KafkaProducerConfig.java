package com.sha.springbootkafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author sa
 * @date 14.11.2021
 * @time 13:46
 */
@Configuration
public class KafkaProducerConfig
{
    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String PRODUCER_BROKERS;

    @Bean
    public KafkaTemplate<String, Object> kafkaProducerTemplate()
    {
        return new KafkaTemplate<>(producerFactory(false));
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTransactionalProducerTemplate()
    {
        return new KafkaTemplate<>(producerFactory(true));
    }


    private ProducerFactory<String, Object> producerFactory(boolean enableTransaction)
    {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PRODUCER_BROKERS);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        if (enableTransaction)
        {
            configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx");
        }

        return new DefaultKafkaProducerFactory<>(configProps);
    }
}
