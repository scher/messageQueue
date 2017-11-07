package com.example;

import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.canva.sqs.local.memory.InMemoryQueueService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Properties;

import static com.google.common.collect.testing.Helpers.assertContains;
import static com.google.common.collect.testing.Helpers.assertEmpty;
import static org.junit.Assert.assertEquals;

public class InMemoryQueueTest {


    // Queue messages tests
    @Test
    public void test() {

    }

    // Queue management tests
    @Test
    public void testCreateQueue() {
        Properties props = Mockito.mock(Properties.class);
        InMemoryQueueService service = new InMemoryQueueService(props);

        ListQueuesResult result = service.listQueues();
        Assert.assertTrue(result.getQueueUrls().isEmpty());

        String newQueueName = "queue";
        CreateQueueResult queueCreationRes = service.createQueue(newQueueName);

        assertEquals(queueCreationRes.getQueueUrl(), newQueueName);
    }

    @Test
    public void testCreateQueueAlreadyExists() {
        Properties props = Mockito.mock(Properties.class);
        InMemoryQueueService service = new InMemoryQueueService(props);

        ListQueuesResult result = service.listQueues();
        Assert.assertTrue(result.getQueueUrls().isEmpty());

        String newQueueName = "queue";
        service.createQueue(newQueueName);
        CreateQueueResult queueCreationResult = service.createQueue(newQueueName);
        assertEquals(queueCreationResult.getQueueUrl(), newQueueName);

        result = service.listQueues();
        assertEquals(result.getQueueUrls().size(), 1);
        assertContains(result.getQueueUrls(), newQueueName);
    }

    @Test
    public void testCreateAndDeleteQueue() {
        Properties props = Mockito.mock(Properties.class);
        InMemoryQueueService service = new InMemoryQueueService(props);
        String newQueueName = "queue";
        service.createQueue(newQueueName);

        ListQueuesResult listQueue = service.listQueues();
        assertContains(listQueue.getQueueUrls(), newQueueName);

        service.deleteQueue(newQueueName);

        listQueue = service.listQueues();
        assertEmpty(listQueue.getQueueUrls());
    }

    @Test
    public void testDeleteNotExistingQueue() {
        Properties props = Mockito.mock(Properties.class);
        InMemoryQueueService service = new InMemoryQueueService(props);
        String newQueueName = "queue";
        service.createQueue(newQueueName);

        ListQueuesResult listQueue = service.listQueues();
        assertContains(listQueue.getQueueUrls(), newQueueName);

        String notExistingQueue = "notExisting";
        service.deleteQueue(notExistingQueue);

        listQueue = service.listQueues();
        assertContains(listQueue.getQueueUrls(), newQueueName);
        assertEquals(listQueue.getQueueUrls().size(), 1);
    }

    @Test
    public void testGetQueueUrlExistingQueue() {
        Properties props = Mockito.mock(Properties.class);
        InMemoryQueueService service = new InMemoryQueueService(props);
        String newQueueName = "queue";
        service.createQueue(newQueueName);

        GetQueueUrlResult queueUrl = service.getQueueUrl(newQueueName);
        assertEquals(queueUrl.getQueueUrl(), newQueueName);
    }

    @Test(expected = QueueDoesNotExistException.class)
    public void testGetQueueUrlNotExistingQueue() {
        Properties props = Mockito.mock(Properties.class);
        InMemoryQueueService service = new InMemoryQueueService(props);
        String notExisitngQueue = "notExisting";

        service.getQueueUrl(notExisitngQueue);
    }
}
