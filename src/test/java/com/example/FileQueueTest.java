package com.example;

import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.canva.sqs.local.filesystem.FileQueueService;
import com.google.common.collect.testing.Helpers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Comparator;
import java.util.Properties;

import static com.canva.sqs.local.filesystem.FileQueueService.SQS_QUEUES_DIR_KEY;
import static com.canva.sqs.local.memory.InMemoryQueue.INFLIGHT_TIMEOUT_SECONDS_KEY;
import static org.junit.Assert.assertEquals;

public class FileQueueTest extends TestCases {

    private Path tempDirectory;
    private Clock clockMock;

//    class TestClock extends Clock {

    @Before
    public void init() throws IOException {
        Properties props = new Properties();
        String delay = String.valueOf(10);
        tempDirectory = Files.createTempDirectory("sqs_tests");

        clockMock = Mockito.mock(Clock.class);
//        Mockito.when(clockMock.millis()).thenReturn(Long);

        props.setProperty(INFLIGHT_TIMEOUT_SECONDS_KEY, delay);
        props.setProperty(SQS_QUEUES_DIR_KEY, tempDirectory.toString());


        service = new FileQueueService(props, clockMock);

        String queueName = "queueName";
        CreateQueueResult queue = service.createQueue(queueName);
        queueUrl = queue.getQueueUrl();
    }

    // Queue messages tests
    @Test
    public void testInvalidationAddsToHead() {
        service.sendMessage(queueUrl, "body");
//        service.sendMessage(queueUrl, "body1");
        ReceiveMessageResult receiveMessageResult = service.receiveMessage(queueUrl);

        ReceiveMessageResult receiveMessageResult2 = service.receiveMessage(queueUrl);
        Helpers.assertEmpty(receiveMessageResult2.getMessages());

        Mockito.when(clockMock.millis()).thenReturn(Long.MAX_VALUE);
        String receiptHandle = receiveMessageResult.getMessages().get(0).getReceiptHandle();
//        service.invalidateNow(queueUrl, receiptHandle);

        ReceiveMessageResult invalidatedMessageResult = service.receiveMessage(queueUrl);

        assertEquals(
                receiveMessageResult.getMessages().get(0).getMessageId(),
                invalidatedMessageResult.getMessages().get(0).getMessageId()
        );
    }

    @After
    public void cleanup() {
        try {
            Files.walk(tempDirectory, FileVisitOption.FOLLOW_LINKS)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
