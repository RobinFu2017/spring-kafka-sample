package com.robin.spring.kafka.config;

import com.robin.spring.kafka.domain.Greeting;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaBatchConsumerConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${kafka.consumer.max-poll-records}")
    private int maxPollRecords;

    @Value(value = "${kafka.consumer.enable-auto-commit}")
    private boolean enableAutoCommit;

    @Value(value = "${kafka.consumer.auto-commit-interval}")
    private int autoCommitInterval;

    @Value("${kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;


    @Bean(name = "batchGreetingKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Greeting> batchGreetingKafkaListenerContainerFactory() {
        return kafkaListenerContainerFactory("batch-greeting", bootstrapAddress, maxPollRecords, true, 10);
    }


    public ConcurrentKafkaListenerContainerFactory<String, Greeting> kafkaListenerContainerFactory(
            String groupId, String bootstrapAddress, int maxPollRecords, boolean batch, int concurrency) {
        ConcurrentKafkaListenerContainerFactory<String, Greeting> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(groupId, bootstrapAddress, maxPollRecords));
        factory.setConcurrency(concurrency);
        factory.setBatchListener(batch);
        return factory;
    }


    public ConsumerFactory<String, Greeting> consumerFactory(String groupId, String bootstrapAddress, int maxPollRecords) {
        Map<String, Object> props = new HashMap<>(10);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }
}
