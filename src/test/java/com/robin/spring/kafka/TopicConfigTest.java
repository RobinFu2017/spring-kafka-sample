package com.robin.spring.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonList;


@RunWith(SpringRunner.class)
@SpringBootTest(classes = KafkaApplication.class)
@Slf4j
public class TopicConfigTest {

    public static final String TOPIC = "robin-topic";

    @Autowired
    private AdminClient adminClient;


    @Test
    public void createTopic() throws ExecutionException, InterruptedException {
        NewTopic topic = new NewTopic(TOPIC, 20, (short) 1);
        Map<String, String> configs = new HashMap<>();
        configs.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, "30000");
        topic.configs(configs);
        CreateTopicsResult topics = adminClient.createTopics(singletonList(topic));
        Void done = topics.all().get();
        log.info("done.");
    }

    @Test
    public void describeTopics() {
        DescribeTopicsResult result = adminClient.describeTopics(singletonList(TOPIC));
        log.info("describe for topic :{},is :{}", TOPIC, result.toString());
    }

}