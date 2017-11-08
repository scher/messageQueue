package com.example;

import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.canva.sqs.local.memory.InMemoryQueueService;
import org.junit.Before;

import java.util.Properties;

import static com.canva.sqs.local.memory.InMemoryQueue.INFLIGHT_TIMEOUT_SECONDS_KEY;

public class InMemoryQueueTest extends TestCases {

    @Before
    public void init() {
        Properties props = new Properties();
        String delay = String.valueOf(10);
        props.setProperty(INFLIGHT_TIMEOUT_SECONDS_KEY, delay);
        service = new InMemoryQueueService(props);
        String queueName = "queueName";
        CreateQueueResult queue = service.createQueue(queueName);
        queueUrl = queue.getQueueUrl();
    }
}
