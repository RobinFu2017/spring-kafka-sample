package com.robin.spring.kafka.listener;

import com.robin.spring.kafka.domain.Greeting;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.CompletableFuture.runAsync;

@Component
public class MessageListener {
    public CountDownLatch latch = new CountDownLatch(3);

    public CountDownLatch partitionLatch = new CountDownLatch(2);

    public CountDownLatch filterLatch = new CountDownLatch(2);

    public CountDownLatch greetingLatch = new CountDownLatch(1);

    @KafkaListener(topics = "${message.topic.name}", groupId = "foo", containerFactory = "fooKafkaListenerContainerFactory")
    public void listenGroupFoo(String message) {
        System.out.println("Received Messasge in group 'foo': " + message);
        latch.countDown();
    }

    @KafkaListener(topics = "${message.topic.name}", groupId = "bar", containerFactory = "barKafkaListenerContainerFactory")
    public void listenGroupBar(String message) {
        System.out.println("Received Messasge in group 'bar': " + message);
        latch.countDown();
    }

    @KafkaListener(topics = "${message.topic.name}", containerFactory = "headersKafkaListenerContainerFactory")
    public void listenWithHeaders(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("Received Messasge: " + message + " from partition: " + partition);
        latch.countDown();
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "${partitioned.topic.name}", partitions = {"0", "3"}))
    public void listenToParition(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("Received Message: " + message + " from partition: " + partition);
        this.partitionLatch.countDown();
    }

    @KafkaListener(topics = "${filtered.topic.name}", containerFactory = "filterKafkaListenerContainerFactory")
    public void listenWithFilter(String message) {
        System.out.println("Recieved Message in filtered listener: " + message);
        this.filterLatch.countDown();
    }

    @KafkaListener(topics = "${greeting.topic.name}", containerFactory = "greetingKafkaListenerContainerFactory")
    public void greetingListener(Greeting greeting) {
        System.out.println("Recieved greeting message: " + greeting);
        this.greetingLatch.countDown();
    }


    @KafkaListener(topics = "${greeting.topic.name}", containerFactory = "batchGreetingKafkaListenerContainerFactory")
    public void batchGreetingListener(List<ConsumerRecord<String, Greeting>> records) {
        System.out.println("batch consume records. size:" + records.size());
        //batch consume kafka message, run async ,then await all.
        CompletableFuture[] futures = records.stream()
                .map(record -> runAsync(() -> listen(record)))
                .peek(v -> sleep(50))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
    }

    private void sleep(long mili) {
        try {
            Thread.sleep(mili);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void listen(ConsumerRecord<String, Greeting> record) {
        Greeting greeting = record.value();
        System.out.println("Recieved greeting message: " + greeting);
        this.greetingLatch.countDown();
    }
}
