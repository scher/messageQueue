package com.example;

import com.amazonaws.services.sqs.model.*;
import com.canva.sqs.QueueService;
import com.canva.sqs.local.memory.InMemoryQueueService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Properties;

import static com.google.common.collect.testing.Helpers.assertContains;
import static com.google.common.collect.testing.Helpers.assertEmpty;
import static org.junit.Assert.*;

/**
 * @author Alexander Pronin
 * @since 07/11/2017
 */
public abstract class TestCases {

    QueueService service;
    String queueUrl;

    // Queue messages tests
    @Test
    public void testInvalidationAddsToHead() {
        service.sendMessage(queueUrl, "body");
        service.sendMessage(queueUrl, "body1");
        ReceiveMessageResult receiveMessageResult = service.receiveMessage(queueUrl);

        String receiptHandle = receiveMessageResult.getMessages().get(0).getReceiptHandle();
        service.invalidateNow(queueUrl, receiptHandle);

        ReceiveMessageResult invalidatedMessageResult = service.receiveMessage(queueUrl);

        assertEquals(
                receiveMessageResult.getMessages().get(0).getMessageId(),
                invalidatedMessageResult.getMessages().get(0).getMessageId()
        );
    }

    @Test
    public void testDeleteMessageAfterInvalidationDoesNotWork() {
        service.sendMessage(queueUrl, "body");
        ReceiveMessageResult receiveMessageResult = service.receiveMessage(queueUrl);

        String receiptHandle = receiveMessageResult.getMessages().get(0).getReceiptHandle();
        service.invalidateNow(queueUrl, receiptHandle);
        service.deleteMessage(queueUrl, receiptHandle);

        ReceiveMessageResult invalidatedMessageResult = service.receiveMessage(queueUrl);

        assertEquals(
                receiveMessageResult.getMessages().get(0).getMessageId(),
                invalidatedMessageResult.getMessages().get(0).getMessageId()
        );
    }

    @Test
    public void testDeleteMessageReallyDeletes() {
        service.sendMessage(queueUrl, "body");
        ReceiveMessageResult receiveMessageResult = service.receiveMessage(queueUrl);

        String receiptHandle = receiveMessageResult.getMessages().get(0).getReceiptHandle();
        service.deleteMessage(queueUrl, receiptHandle);
        service.invalidateNow(queueUrl, receiptHandle);

        ReceiveMessageResult invalidatedMessageResult = service.receiveMessage(queueUrl);

        assertEmpty(invalidatedMessageResult.getMessages());
    }

    @Test
    public void testInvalidationChangesReceiptHandler() {
        service.sendMessage(queueUrl, "body");
        ReceiveMessageResult receiveMessageResult = service.receiveMessage(queueUrl);
        service.invalidateNow(queueUrl, receiveMessageResult.getMessages().get(0).getReceiptHandle());
        ReceiveMessageResult invalidatedMessageResult = service.receiveMessage(queueUrl);

        assertNotEquals(
                receiveMessageResult.getMessages().get(0).getReceiptHandle(),
                invalidatedMessageResult.getMessages().get(0).getReceiptHandle()
        );
    }

    @Test
    public void testInvalidationNow() {
        service.sendMessage(queueUrl, "body");
        ReceiveMessageResult receiveMessageResult = service.receiveMessage(queueUrl);
        service.invalidateNow(queueUrl, receiveMessageResult.getMessages().get(0).getReceiptHandle());
        ReceiveMessageResult invalidatedMessageResult = service.receiveMessage(queueUrl);

        assertEquals(
                receiveMessageResult.getMessages().get(0).getMessageId(),
                invalidatedMessageResult.getMessages().get(0).getMessageId()
        );
    }

    @Test
    public void testDeleteMessageWrongHandler() {
        service.sendMessage(queueUrl, "body");
        ReceiveMessageResult receiveMessageResult1 = service.receiveMessage(queueUrl);

        String wrong = "dummy";
        service.deleteMessage(queueUrl, wrong);
        service.invalidateNow(queueUrl, receiveMessageResult1.getMessages().get(0).getReceiptHandle());

        ReceiveMessageResult receiveMessageResult2 = service.receiveMessage(queueUrl);

        assertEquals(
                receiveMessageResult1.getMessages().get(0).getMessageId(),
                receiveMessageResult2.getMessages().get(0).getMessageId()
        );
    }

    @Test
    public void testReceiveMessageBecomesInvisible() {
        String messageBody1 = "body";
        service.sendMessage(queueUrl, messageBody1);
        String messageBody2 = "body1";
        service.sendMessage(queueUrl, messageBody2);
        ReceiveMessageResult receiveMessageResult1 = service.receiveMessage(queueUrl);
        ReceiveMessageResult receiveMessageResult2 = service.receiveMessage(queueUrl);

        assertNotEquals(
                receiveMessageResult1.getMessages().get(0).getMessageId(),
                receiveMessageResult2.getMessages().get(0).getMessageId()
        );
    }

    @Test
    public void testReceiveMessageSetsReceiptHandle() {
        service.sendMessage(queueUrl, "body");
        ReceiveMessageResult result = service.receiveMessage(queueUrl);
        assertFalse(result.getMessages().get(0).getReceiptHandle().isEmpty());
    }

    @Test
    public void testReceiveMessageQueueIsEmpty() {
        ReceiveMessageResult receiveMessageResult = service.receiveMessage(queueUrl);
        assertEmpty(receiveMessageResult.getMessages());
    }

    @Test
    public void testReceiveMessageQueueDoesNotExists() {
        Properties props = new Properties();
        InMemoryQueueService service = new InMemoryQueueService(props);

        ReceiveMessageResult result = service.receiveMessage("notExistent");
        assertEmpty(result.getMessages());
    }

    @Test
    public void testReceiveMessageFIFO() {
        String messageBody1 = "body1";
        service.sendMessage(queueUrl, messageBody1);
        String messageBody2 = "body";
        service.sendMessage(queueUrl, messageBody2);

        ReceiveMessageResult receiveMessageResult = service.receiveMessage(queueUrl);
        assertEquals(receiveMessageResult.getMessages().get(0).getBody(), messageBody1);
    }

    @Test
    public void testSendMessageQueueDoesNotExists() {
        Properties props = new Properties();
        InMemoryQueueService service = new InMemoryQueueService(props);

        SendMessageResult sendMessageResult = service.sendMessage("notExistent", "body");
        assertNull(sendMessageResult.getMessageId());
    }

    @Test
    public void testSendMessageSetsMessageId() {
        SendMessageResult sendMessageResult = service.sendMessage(queueUrl, "body");
        ReceiveMessageResult receiveMessageResult = service.receiveMessage(queueUrl);
        assertEquals(sendMessageResult.getMessageId(), receiveMessageResult.getMessages().get(0).getMessageId());
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
