package com.robin.spring.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = KafkaApplication.class)
public class ManualConsumeTest {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ManualConsumeTest.class);
    public static final String TOPIC = "robin";


    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    @Qualifier("fooKafkaListenerContainerFactory")
    private ConcurrentKafkaListenerContainerFactory<String, String> factory;

    private Executor executor = Executors.newFixedThreadPool(30);
    private CountDownLatch latch = new CountDownLatch(100);

    @Test
    public void whenSpringContextIsBootstrapped_thenNoExceptions() throws InterruptedException {
        CompletableFuture.runAsync(this::consume, executor);

        log.info("start send message.");
        IntStream.range(0, 100)
                .forEach(i -> {
                    kafkaTemplate.send(TOPIC, "hi:" + i);
                });

        latch.await(30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private void consume() {
        log.info("start consume.");
        ConsumerFactory<String, String> consumerFactory = (ConsumerFactory<String, String>) factory.getConsumerFactory();
        Consumer<String, String> consumer = consumerFactory.createConsumer("group-a", "id-1");
        consumer.subscribe(singletonList(TOPIC));
        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(30));
            poll.forEach(record -> {
                latch.countDown();
                log.info("record:{}", record);
            });
        }
    }


}
